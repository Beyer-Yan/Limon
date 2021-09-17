package main

import "math"
import "fmt"
//import "sync"
//import "os"
//import "strconv"
//import "container/list"

var gOpt = 10000

var B = 0.25
var PS = 512
var CS = 1024*1024
var MS = 24
var s0 = 256

var g_Gs = 0

type searchNode struct {
	path []int
}

func lcm(iptA int, iptB int) int{
	a := iptA
	b := iptB
	c := 0
    for {
		if (b != 0){
			c = a % b
			a = b
			b = c
		}else{
			break
		}
	} 
	// then calc the lcm
	return  int(iptA * iptB / a)
}

func gcd(iptA int, iptB int) int{
	tmp := 0
	if iptA < iptB{
        tmp = iptA 
        iptB = iptA
        iptA = tmp
	}
 
	for{
		if (iptB > 0){
			tmp = iptA % iptB
			iptA = iptB
			iptB = tmp
		}else{
			break
		}
	}
    return iptA
}

func calc_Gs() int {
	var m = int(B*float64(CS));
	for ;m%4!=0;m-- {}
	return m
}

func bound(val int) int{
	cnt := 1
	for{
		tmp := int(math.Floor(float64(val) * (1+B)))
		if (tmp>g_Gs){
			break
		}else{
			val = tmp
			cnt++
		}
	}
	//fmt.Println("val, bound,",val, cnt)
	return cnt
}

func treaceSolution(node *searchNode) {
	fmt.Printf("%d: ",len(node.path))
	fmt.Print("[ ")
	for _, val := range(node.path){
		fmt.Print(val,",")
	}
	fmt.Println("]")
}

func checkValidity(solvedCnt int,val int) bool{
	if(val>g_Gs){
		return false
	}

	if(val%4!=0){
		return false
	}
		
	if (val%PS==0 ){
			return true
	}

	ri := val%PS
	gi := PS%ri
	minGap := gcd(ri,gi)
	//fmt.Println("minGap,", minGap)
	if (minGap>=MS && bound(val)+solvedCnt <gOpt){
		return true
	}
	return false
}

func findSolution(node *searchNode) {
	pathLen := len(node.path)
	val := node.path[pathLen-1]
	maxVal := int(math.Floor(float64(val)*(1+B)))
	//fmt.Println("val, maxVal, pathLen", val,maxVal, pathLen)

	for{
		if (maxVal>val){
			if (checkValidity(pathLen,maxVal)){
				curNode := &searchNode{
					path : make([]int,pathLen+1),
				}
				copy(curNode.path,node.path)
				curNode.path[pathLen] = maxVal
				findSolution(curNode)
			} else if(maxVal>g_Gs){
				// Now I terminate
				if(pathLen+1 < gOpt){
					gOpt = pathLen+1
					treaceSolution(node)
				}
			}
			maxVal--
		}else{
			break
		}
	}
}

func main() {
	
	g_Gs = calc_Gs()
	fmt.Println("Gs: ",g_Gs)
	initNode := &searchNode{
		path : make([]int, 1),
	}
	initNode.path[0] = s0

	findSolution(initNode)

	fmt.Println("searching completes")
}