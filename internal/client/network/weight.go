package network

func gcd(x, y uint64) uint64 {
	for y != 0 {
		x, y = y, x%y
	}
	return x
}

func normalizeWeights(r []*upstream) {

	for _, u := range r {
		if u.weight == 0 {
			u.weight = 1
		}
	}

	if len(r) < 2 {
		return
	}
	weightsGcd := r[0].weight
	for i := 1; i < len(r); i++ {
		weightsGcd = gcd(weightsGcd, r[i].weight)
	}
	for i := range r {
		r[i].weight = r[i].weight / weightsGcd
	}
}
