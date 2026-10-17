//go:build !linux && !darwin && !windows

package service

// Fallback for a GOOS with no service-manager integration. The build still
// compiles and the CLI still has a `service` subcommand, which reports honestly
// that this platform has none - better than a subcommand that silently vanishes
// from the help on one build and appears on another.

import "context"

// Install reports that this platform has no service integration.
func Install(context.Context, Deps) (Report, error) { return Report{}, ErrUnsupported }

// Uninstall reports that this platform has no service integration.
func Uninstall(context.Context, Deps) (Report, error) { return Report{}, ErrUnsupported }

// Status reports that this platform has no service integration.
func Status(context.Context, Deps) (Report, error) { return Report{}, ErrUnsupported }

// Start reports that this platform has no service integration.
func Start(context.Context, Deps) (Report, error) { return Report{}, ErrUnsupported }

// Stop reports that this platform has no service integration.
func Stop(context.Context, Deps) (Report, error) { return Report{}, ErrUnsupported }

// Restart reports that this platform has no service integration.
func Restart(context.Context, Deps) (Report, error) { return Report{}, ErrUnsupported }
