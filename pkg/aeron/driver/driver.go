package driver

import (
	"fmt"
	"runtime"

	"github.com/ebitengine/purego"
)

var libHandle uintptr

// Open loads the Aeron C client shared library and registers all function symbols.
func Open(libPath string) error {
	if libPath == "" {
		switch runtime.GOOS {
		case "linux":
			libPath = "libaeron.so"
		case "darwin":
			libPath = "libaeron.dylib"
		default:
			return fmt.Errorf("unsupported OS: %s", runtime.GOOS)
		}
	}

	handle, err := purego.Dlopen(libPath, purego.RTLD_NOW|purego.RTLD_GLOBAL)
	if err != nil {
		return fmt.Errorf("dlopen %s: %w", libPath, err)
	}
	libHandle = handle

	registerSymbols(handle)
	return nil
}

// Close releases the shared library handle.
func Close() error {
	if libHandle != 0 {
		if err := purego.Dlclose(libHandle); err != nil {
			return err
		}
		libHandle = 0
	}
	return nil
}

// Handle returns the raw library handle for direct Dlsym calls.
func Handle() uintptr {
	return libHandle
}
