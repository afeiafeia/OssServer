package log

type Level uint32

const (
	FATAL Level = iota
	ERROR
	WARN
	INFO
	DEBUG
	TRACE
)

func (l Level) String() string {
	return [...]string{
		"FATAL",
		"ERROR",
		"WARN",
		"INFO",
		"DEBUG",
		"TRACE",
	}[l]
}

func ToLevel(lv string) Level {
	m := map[string]Level{
		"FATAL": FATAL,
		"ERROR": ERROR,
		"WARN":  WARN,
		"INFO":  INFO,
		"DEBUG": DEBUG,
		"TRACE": TRACE,
	}
	return m[lv]
}
