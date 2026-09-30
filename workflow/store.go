package workflow

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"
)

// Store 抽象工作流状态的持久化。
type Store interface {
	// Load 读取状态；不存在时返回 (nil, nil)。
	Load(workflowID string) (*WorkflowState, error)
	// Save 整体覆盖保存状态。
	Save(state *WorkflowState) error
}

// newState 依据 spec 构造一份全新状态。
func newState(spec *WorkflowSpec) *WorkflowState {
	st := &WorkflowState{
		WorkflowID: spec.WorkflowID,
		Status:     WFRunning,
		Tasks:      make(map[string]*TaskState, len(spec.Tasks)),
		UpdatedAt:  time.Now(),
	}
	for _, t := range spec.Tasks {
		st.Tasks[t.Name] = &TaskState{
			Name:   t.Name,
			Status: StatusPending,
		}
	}
	return st
}

// MemoryStore 纯内存存储（用于测试与同进程内的重复提交幂等校验）。
type MemoryStore struct {
	data map[string]*WorkflowState
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{data: make(map[string]*WorkflowState)}
}

func (s *MemoryStore) Load(workflowID string) (*WorkflowState, error) {
	v, ok := s.data[workflowID]
	if !ok {
		return nil, nil
	}
	// 返回深拷贝，避免外部绕过引擎直接改状态
	return cloneState(v), nil
}

func (s *MemoryStore) Save(state *WorkflowState) error {
	s.data[state.WorkflowID] = cloneState(state)
	return nil
}

func cloneState(in *WorkflowState) *WorkflowState {
	if in == nil {
		return nil
	}
	b, err := json.Marshal(in)
	if err != nil {
		// 状态只含可序列化的基本类型，这里不应失败
		panic(err)
	}
	var out WorkflowState
	if err := json.Unmarshal(b, &out); err != nil {
		panic(err)
	}
	return &out
}

// FileStore 以单个 JSON 文件作为持久化介质，模拟进程重启后的恢复。
// 文件名固定为 <dir>/<workflowID>.json。
type FileStore struct {
	dir string
}

func NewFileStore(dir string) *FileStore {
	return &FileStore{dir: dir}
}

func (s *FileStore) path(workflowID string) string {
	return filepath.Join(s.dir, filepath.Base(workflowID)+".json")
}

func (s *FileStore) Load(workflowID string) (*WorkflowState, error) {
	b, err := os.ReadFile(s.path(workflowID))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var st WorkflowState
	if err := json.Unmarshal(b, &st); err != nil {
		return nil, err
	}
	if st.Tasks == nil {
		st.Tasks = make(map[string]*TaskState)
	}
	return &st, nil
}

func (s *FileStore) Save(state *WorkflowState) error {
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return err
	}
	b, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path(state.WorkflowID) + ".tmp"
	// 先写临时文件再原子重命名，避免中断时留下半个状态文件
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path(state.WorkflowID))
}
