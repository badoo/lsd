package util

// given a slice of strings `haystack', look for string `needle' in it
// position has no meaning if found == false
func StrSliceFind(haystack []string, needle string) (found bool, position int) {
	for i, val := range haystack {
		if val == needle {
			return true, i
		}
	}
	return false, -1
}

func StrSliceEqual(lhs, rhs []string) bool {
	if len(lhs) != len(rhs) {
		return false
	}

	for _, val := range lhs {
		found, _ := StrSliceFind(rhs, val)
		if !found {
			return false
		}
	}
	return true
}

func test() {}
