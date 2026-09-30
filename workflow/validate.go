package workflow

import "sort"

// validate 校验工作流静态定义：重名、缺失依赖、环。
// 错误彼此可区分：*DuplicateTaskError / *MissingDependencyError / *CycleError。
func validate(spec *WorkflowSpec) error {
	exists := make(map[string]bool, len(spec.Tasks))
	for _, t := range spec.Tasks {
		if exists[t.Name] {
			return &DuplicateTaskError{Task: t.Name}
		}
		exists[t.Name] = true
	}

	deps := make(map[string][]string, len(spec.Tasks))
	for _, t := range spec.Tasks {
		var missing []string
		seen := make(map[string]bool)
		var uniq []string
		for _, d := range t.Dependencies {
			if !exists[d] {
				missing = append(missing, d)
			}
			if !seen[d] {
				seen[d] = true
				uniq = append(uniq, d)
			}
		}
		if len(missing) > 0 {
			return &MissingDependencyError{Task: t.Name, Deps: missing}
		}
		deps[t.Name] = uniq
	}

	if cyc := findCycle(deps); cyc != nil {
		return &CycleError{Cycle: cyc}
	}
	return nil
}

// findCycle 用 DFS 三色标记法返回一条环路径（起点在末尾重复出现）。
func findCycle(deps map[string][]string) []string {
	const white, gray, black = 0, 1, 2
	color := make(map[string]int, len(deps))
	var stack []string

	var dfs func(n string) []string
	dfs = func(n string) []string {
		color[n] = gray
		stack = append(stack, n)
		for _, d := range deps[n] {
			switch color[d] {
			case gray:
				// 找到回边：从栈中 d 的位置开始构成环
				i := 0
				for stack[i] != d {
					i++
				}
				cyc := append([]string{}, stack[i:]...)
				return append(cyc, d)
			case white:
				if p := dfs(d); p != nil {
					return p
				}
			}
		}
		stack = stack[:len(stack)-1]
		color[n] = black
		return nil
	}

	// 固定遍历顺序，保证结果可复现
	names := make([]string, 0, len(deps))
	for n := range deps {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		if color[n] == white {
			if p := dfs(n); p != nil {
				return p
			}
		}
	}
	return nil
}
