package driver

import "fmt"

// AeronError represents an error returned by the Aeron C client.
type AeronError struct {
	Code    int32
	Message string
}

func (e *AeronError) Error() string {
	return fmt.Sprintf("aeron error %d: %s", e.Code, e.Message)
}

// CheckResult checks a C function return code. Negative = error.
func CheckResult(result int32) error {
	if result < 0 {
		return &AeronError{
			Code:    aeronErrcode(),
			Message: aeronErrmsg(),
		}
	}
	return nil
}
