package service

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// utf8BOM is the byte sequence Windows PowerShell 5.1 needs at the head of a .ps1 file
// before it will read the rest as UTF-8.
const utf8BOM = "\xEF\xBB\xBF"

// TestShippedInstallerCarriesUTF8BOM pins the encoding of the installer the product hands to
// PowerShell.
//
// PowerShell 7 (pwsh) defaults to UTF-8 and reads a BOM-less script correctly. Windows
// PowerShell 5.1 does not: it falls back to the machine's ANSI code page, so a non-ASCII
// character decodes into replacement bytes - and when one of those lands inside a
// double-quoted string, as an em dash in the installer's own error text did, the byte 0x94
// becomes a closing curly quote that 5.1 treats as a string delimiter. The whole file then
// fails to parse, with an error pointing at whatever token happens to follow.
//
// That is not cosmetic. scm_windows.go invokes `powershell`, which IS 5.1 on a stock Windows
// install, so `synosync service uninstall` and `service status` failed outright; and `service
// install` deliberately refuses and prints a command for the user to paste into an elevated
// PowerShell, which for most people is also 5.1, so the documented remedy hit the same error.
// CI ran the script's self-test under pwsh only, so all of it stayed green.
//
// A BOM fixes both hosts and costs three bytes. This test guards it because nothing else would
// notice its loss - an editor rewriting the file is exactly how it would go missing, and the
// failure only appears on a real Windows box.
func TestShippedInstallerCarriesUTF8BOM(t *testing.T) {
	body, err := template("install-service.ps1")
	if err != nil {
		t.Fatalf("template: %v", err)
	}
	if !strings.HasPrefix(body, utf8BOM) {
		t.Fatal("the embedded installer does not start with a UTF-8 BOM; Windows PowerShell 5.1 " +
			"will read its non-ASCII characters in the ANSI code page and fail to parse the file")
	}
}

// TestSelfTestScriptCarriesUTF8BOM holds the development self-test to the same rule. It is
// deliberately NOT embedded (TestDevelopmentSelfTestIsNotShipped), so it is read from disk -
// and it needs the BOM for the same reason plus one more: CI now runs it under BOTH hosts, and
// a run under 5.1 is the only thing that would have caught the installer's own breakage.
func TestSelfTestScriptCarriesUTF8BOM(t *testing.T) {
	path := filepath.Join("templates", "install-service_test.ps1")
	body, err := os.ReadFile(path) // #nosec G304 -- a fixed path inside the package
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if !strings.HasPrefix(string(body), utf8BOM) {
		t.Fatalf("%s does not start with a UTF-8 BOM", path)
	}
}
