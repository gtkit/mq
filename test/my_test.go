package test

import (
	"fmt"
	"testing"
)

func TestChannel(t *testing.T) {
	var a = make(chan struct{})
	//fmt.Println("-----------a 1", <-a)
	close(a)
	v := <-a
	fmt.Println("-----------a 2: ", v)
	fmt.Println("-----------a 3: ", v == struct{}{})
}
