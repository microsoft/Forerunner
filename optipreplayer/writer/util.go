// Copyright (c) 2021 Microsoft Corporation. 
 // Licensed under the GNU General Public License v3.0.

package writer

import (
	"encoding/json"
	"io/ioutil"
	"runtime"
	"strings"

	"github.com/pkg/errors"
)

func ProtectRun(entry func() error) error {
	defer func() {
		err := recover()
		if err != nil {
			switch err.(type) {
			default:
			}
		}
	}()
	return entry()
}

func SubStr(s string, pos, length int) string {
	runes := []rune(s)
	l := pos + length
	if l > len(runes) {
		l = len(runes)
	}
	return string(runes[pos:l])
}

func GetParentDirectory(dirctory string) string {
	return SubStr(dirctory, 0, strings.LastIndex(dirctory, "/"))
}

func CurrentFile() string {
	_, file, _, ok := runtime.Caller(1)
	if !ok {
		panic(errors.New("Can not get current file info"))
	}
	return file
}

func Load(filename string, v interface{}) {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		return
	}

	err = json.Unmarshal(data, v)
	if err != nil {
		return
	}
}
