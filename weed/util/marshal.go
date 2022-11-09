package util

import "encoding/json"

func IndentString(x interface{}) string {
	s, _ := json.MarshalIndent(x, "", " ")
	return string(s)
}

func String(x interface{}) string {
	s, _ := json.Marshal(x)
	return string(s)
}
