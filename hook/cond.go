package hook

type CondF struct {
	cond  func() bool
	fun   func()
	wanna bool
}

func NewCondF(cond func() bool) *CondF {
	return &CondF{
		cond:  cond,
		fun:   func() {},
		wanna: false,
	}
}

func (cf *CondF) ReinitCondF(cond func() bool) *CondF {
	if cf == nil {
		return NewCondF(cond)
	}

	cf.cond = cond
	cf.fun = func() {}
	cf.wanna = false
	return cf
}

func (cf *CondF) Call(f func()) bool {
	if cf.cond() {
		if cf.wanna {
			cf.wanna = false
			cf.fun()
		}
		cf.wanna = false
		f()
		cf.fun = func() {}

		return true
	} else {
		oldFun := cf.fun
		cf.fun = func() {
			oldFun()
			f()
		}
		cf.wanna = true

		return false
	}
}

func (cf *CondF) Recall() bool {
	if cf.wanna && cf.cond() {
		cf.wanna = false
		cf.fun()
		cf.fun = func() {}

		return true
	}

	return false
}

func (cf *CondF) AndCond(cond func() bool) {
	oldCond := cf.cond
	cf.cond = func() bool {
		return oldCond() && cond()
	}
}

func (cf *CondF) OrCond(cond func() bool) {
	oldCond := cf.cond
	cf.cond = func() bool {
		return oldCond() || cond()
	}
}

type OptCondF struct {
	cond     func() bool
	funs     []func()
	funs_num int
}

func NewOptCondF(cond func() bool) *OptCondF {
	return &OptCondF{
		cond:     cond,
		funs:     nil,
		funs_num: 0,
	}
}

func (cf *OptCondF) ReinitCondF(cond func() bool) *OptCondF {
	if cf == nil {
		return NewOptCondF(cond)
	}

	cf.cond = cond
	cf.funs_num = 0
	return cf
}

func (cf *OptCondF) Call(f func()) bool {
	if cf.cond() {
		for i := 0; i < cf.funs_num; i++ {
			cf.funs[i]()
		}
		f()
		cf.funs_num = 0
		return true
	}
	if cf.funs_num >= len(cf.funs) {
		cf.funs = append(cf.funs, f)
	} else {
		cf.funs[cf.funs_num] = f
	}
	cf.funs_num++
	return false
}

func (cf *OptCondF) Recall() bool {
	if cf.funs_num > 0 && cf.cond() {
		for i := 0; i < cf.funs_num; i++ {
			cf.funs[i]()
		}
		cf.funs_num = 0
		return true
	}
	return false
}
