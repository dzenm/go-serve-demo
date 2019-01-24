package main

import (
	"fmt"
	"sort"
)

func main() {

	nums := []int{3, 4, 9, 90, 12, 16, 87, 8}
	sort.Ints(nums) // 排序函数
	for _, v := range nums {
		fmt.Print(v, " ")
	}
}
