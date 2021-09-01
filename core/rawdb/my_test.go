// Copyright (c) 2021 Microsoft Corporation. 
 // Licensed under the GNU General Public License v3.0.

package rawdb

import (
	"fmt"
	"testing"
)

func A() (ret int) {
	defer func() { ret = ret + 1 }()

	return 123
}

func TestA(t *testing.T) {
	fmt.Println(A())
}
