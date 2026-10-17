package actions

import (
	"errors"
	"fmt"
	"testing"

	"github.com/mfilippov/synosync/internal/auth"
	"github.com/mfilippov/synosync/internal/sproto"
)

func TestErrorForCodeResumeToken(t *testing.T) {
	err := errorForCode(CodeResumeToken)
	if !errors.Is(err, ErrResumeToken) {
		t.Fatalf("err = %v, want errors.Is(ErrResumeToken)", err)
	}
	// The resume-token error must not collide with the other typed sentinels.
	for _, s := range []error{ErrConflictVersion, ErrPathError, ErrHashMismatch, auth.ErrSessionVanished} {
		if errors.Is(err, s) {
			t.Errorf("resume-token code matched unrelated sentinel %v", s)
		}
	}
}

func TestErrorForCodeAllSentinels(t *testing.T) {
	cases := []struct {
		name string
		code int64
		want error
	}{
		{"conflict", CodeConflictVersion, ErrConflictVersion},
		{"path", CodePathError, ErrPathError},
		{"hash", CodeHashMismatch, ErrHashMismatch},
		{"vanished", CodeSessionVanished, auth.ErrSessionVanished},
		{"resume", CodeResumeToken, ErrResumeToken},
		{"client version", CodeClientVersion, auth.ErrClientVersion},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := errorForCode(tc.code); !errors.Is(err, tc.want) {
				t.Fatalf("errorForCode(%d) = %v, want errors.Is(%v)", tc.code, err, tc.want)
			}
		})
	}
}

// TestErrorForCodeUnmappedCarriesCode confirms an unmapped result code surfaces as a typed
// *ServerError whose numeric code a caller can recover: the engine's
// deterministic-rejection escalation compares codes across drains, so the code must survive
// wrapping instead of being formatted into a message. The message itself is unchanged.
func TestErrorForCodeUnmappedCarriesCode(t *testing.T) {
	err := errorForCode(33333)
	if got := err.Error(); got != "actions: server returned error code 33333" {
		t.Errorf("message = %q, want the unchanged generic wording", got)
	}
	code, ok := ServerErrorCode(fmt.Errorf("engine: apply mkdir: %w", err))
	if !ok || code != 33333 {
		t.Errorf("ServerErrorCode through a wrap = (%d, %v), want (33333, true)", code, ok)
	}
	// A mapped code has its own sentinel and dedicated handling: it must NOT read as an
	// unknown deterministic rejection.
	for _, mapped := range []int64{CodeConflictVersion, CodePathError, CodeHashMismatch, CodeResumeToken,
		CodeSessionVanished, CodeClientVersion} {
		if _, ok := ServerErrorCode(errorForCode(mapped)); ok {
			t.Errorf("mapped code %d reported as an unmapped ServerError", mapped)
		}
	}
	// Non-server failures carry no code.
	for _, err := range []error{nil, ErrConnBroken, ErrUnexpectedReply, errors.New("dial: boom")} {
		if _, ok := ServerErrorCode(err); ok {
			t.Errorf("ServerErrorCode(%v) = true, want false", err)
		}
	}
}

// TestNameRejectionCodesAreBothTypedAndNumeric pins the deliberate dual nature of the three
// MEASURED codes that carry a sentinel AND their number: the two name refusals (two-client
// finding H4) and the transient 32772 (finding H5). They must satisfy two contracts at once:
// errors.Is against their sentinel, so a caller can dispatch on the CAUSE, and ServerErrorCode
// reporting their number, because the engine's per-code handling of them is built on the
// identical-code streak that function feeds (engine/servererr.go) - terminal escalation for the
// name refusals, retry pacing for 32772. Mapping them the ordinary way
// - a bare wrapped sentinel in errorForCode - satisfies the first and silently breaks the second,
// turning a terminal name refusal back into a forever-retry and un-pacing the transient one.
func TestNameRejectionCodesAreBothTypedAndNumeric(t *testing.T) {
	cases := []struct {
		name string
		code int64
		want error
		// other is a sibling sentinel, which must NOT match: the causes carry different
		// operator remedies (rename one of a case-pair vs. resolve a file/dir clash vs. wait).
		other error
	}{
		{"case collision", CodeNameCaseCollision, ErrNameCaseCollision, ErrNameTypeCollision},
		{"type collision", CodeNameTypeCollision, ErrNameTypeCollision, ErrNameCaseCollision},
		{"node busy", CodeNodeBusy, ErrNodeBusy, ErrNameCaseCollision},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := errorForCode(tc.code)
			wrapped := fmt.Errorf("engine: apply mkdir: %w", err)
			if !errors.Is(wrapped, tc.want) {
				t.Errorf("errorForCode(%d) = %v, want errors.Is(%v) through a wrap", tc.code, err, tc.want)
			}
			if errors.Is(err, tc.other) {
				t.Errorf("errorForCode(%d) also matched the sibling sentinel %v", tc.code, tc.other)
			}
			code, ok := ServerErrorCode(wrapped)
			if !ok || code != tc.code {
				t.Errorf("ServerErrorCode = (%d, %v), want (%d, true): the escalation counts the number",
					code, ok, tc.code)
			}
			// A *ServerError constructed directly (as the engine's fakes and the escalation's
			// tests do) carries the sentinel too, because it is derived from the code rather
			// than stored in a field.
			if !errors.Is(&ServerError{Code: tc.code}, tc.want) {
				t.Errorf("a hand-built *ServerError{Code: %d} does not match %v", tc.code, tc.want)
			}
			// The message keeps the generic numeric wording (log lines and trace assertions
			// depend on it); the cause travels as the sentinel.
			if got, want := err.Error(), fmt.Sprintf("actions: server returned error code %d", tc.code); got != want {
				t.Errorf("message = %q, want %q", got, want)
			}
		})
	}
	// Every other code unwraps to nothing, so no unrelated error accidentally matches a name
	// sentinel (errors.Is walks Unwrap() == nil as "no further cause").
	for _, code := range []int64{33333, 40962, CodeConflictVersion, CodePathError} {
		err := errorForCode(code)
		if errors.Is(err, ErrNameCaseCollision) || errors.Is(err, ErrNameTypeCollision) {
			t.Errorf("errorForCode(%d) = %v matched a name-collision sentinel", code, err)
		}
		if errors.Is(err, ErrNodeBusy) {
			t.Errorf("errorForCode(%d) = %v matched the transient node-busy sentinel", code, err)
		}
	}
}

func TestDecodeResumeToken(t *testing.T) {
	cases := []struct {
		name string
		m    *sproto.Map
		want ResumeToken
	}{
		{
			name: "full resume_token key",
			m: sproto.NewMap().
				Set("resume_token", sproto.String("tok-123")).
				Set("offset", sproto.Int(4096)).
				Set("size", sproto.Int(1<<20)),
			want: ResumeToken{Token: "tok-123", Offset: 4096, Size: 1 << 20},
		},
		{
			name: "fallback token key",
			m: sproto.NewMap().
				Set("token", sproto.String("tok-abc")),
			want: ResumeToken{Token: "tok-abc"},
		},
		{
			name: "resume_token wins over token",
			m: sproto.NewMap().
				Set("resume_token", sproto.String("primary")).
				Set("token", sproto.String("secondary")),
			want: ResumeToken{Token: "primary"},
		},
		{
			name: "absent fields stay zero, extras ignored",
			m: sproto.NewMap().
				Set("extra", sproto.String("ignored")),
			want: ResumeToken{},
		},
		{
			name: "wrong-typed fields ignored",
			m: sproto.NewMap().
				Set("resume_token", sproto.Int(7)).
				Set("offset", sproto.String("nope")),
			want: ResumeToken{},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := decodeResumeToken(tc.m); got != tc.want {
				t.Errorf("decodeResumeToken = %+v, want %+v", got, tc.want)
			}
		})
	}
}

// TestErrorForCodeLiveVerified pins the result-code mapping to what a live DSM
// actually emits. Every code here was triggered deliberately against DSM 7.2 /
// Drive Server 4.0.3 rather than inferred: 45057 in particular replaced a guessed
// 40976 that the server never sends, which left the re-token recovery armed on a
// value that could not occur.
func TestErrorForCodeLiveVerified(t *testing.T) {
	cases := []struct {
		code int64
		want error
		how  string
	}{
		{24579, ErrConflictVersion, "upload to an existing node with an older mtime"},
		{36870, ErrHashMismatch, "upload advertising a hash that is not the body's MD4"},
		{45057, ErrResumeToken, "resume_upload / resume_download with a bogus or absent token"},
		{28672, ErrPathError, "query_node with an empty path; rename of an absent source"},
		{32769, ErrPathError, "query_node / list / download / upload on an absent target"},
		{40961, ErrPathError, "remove / rename that will not apply"},
	}
	for _, tc := range cases {
		err := errorForCode(tc.code)
		if !errors.Is(err, tc.want) {
			t.Errorf("errorForCode(%d) = %v, want %v (observed via: %s)", tc.code, err, tc.want, tc.how)
		}
	}
	// 40962 stays UNMAPPED on purpose: it is the optimistic-concurrency rejection
	// Rename retries on, not a condition callers dispatch a sentinel on.
	var se *ServerError
	if err := errorForCode(40962); !errors.As(err, &se) || se.Code != 40962 {
		t.Errorf("errorForCode(40962) = %v, want a *ServerError carrying the code", err)
	}
}
