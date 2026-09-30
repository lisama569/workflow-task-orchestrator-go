package workflow

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// Store 负责任务运行状态的持久化与重启恢复。
// Save 必须在每次任务状态流转后被调用，从而保证崩溃 / 重启时
// "已完成的任务不会被重复执行"。
type Store interface {
	// Load 读取一个工作流的已持久化快照；不存在时返回 (nil, nil)。
	Load(workflowID string) (*WorkflowSnapshot, error)
	// Save 原子写入一个工作流快照。
	Save(snap *WorkflowSnapshot) error
}

// WorkflowSnapshot 是一次工作流运行的可持久化快照。
type WorkflowSnapshot struct {
	WorkflowID string                `json:"workflow_id"`
	Finished   bool                  `json:"finished"`  // 工作流是否已跑到终态
	Success    bool                  `json:"success"`   // 终态下是否整体成功
	Recovered  bool                  `json:"recovered"` // 是否曾经发生过断点恢复
	Tasks      map[string]*TaskState `json:"tasks"`     // 每个任务的完整状态（含每次尝试）
}

// cloneSnapshot 深拷贝快照，避免调用方与存储内部共享可变状态。
func cloneSnapshot(snap *WorkflowSnapshot) *WorkflowSnapshot {
	if snap == nil {
		return nil
	}
	cp := &WorkflowSnapshot{
		WorkflowID: snap.WorkflowID,
		Finished:   snap.Finished,
		Success:    snap.Success,
		Recovered:  snap.Recovered,
		Tasks:      make(map[string]*TaskState, len(snap.Tasks)),
	}
	for k, v := range snap.Tasks {
		tc := *v
		tc.Attempts = append([]Attempt(nil), v.Attempts...)
		cp.Tasks[k] = &tc
	}
	return cp
}

// MemoryStore：进程内快照存储，用于单元测试与"同一进程内模拟重启"。
type MemoryStore struct {
	mu    sync.Mutex
	snaps map[string]*WorkflowSnapshot
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{snaps: make(map[string]*WorkflowSnapshot)}
}

func (s *MemoryStore) Load(workflowID string) (*WorkflowSnapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap, ok := s.snaps[workflowID]
	if !ok {
		return nil, nil
	}
	return cloneSnapshot(snap), nil
}

func (s *MemoryStore) Save(snap *WorkflowSnapshot) error {
	if snap == nil {
		return errors.New("nil snapshot")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snaps[snap.WorkflowID] = cloneSnapshot(snap)
	return nil
}

// FileStore：基于 JSON 文件的快照存储。
// 每个工作流一个文件，写入采用"临时文件 + rename"原子替换，
// 因此进程在任意时刻崩溃后，重新提交同一 WorkflowID 都能读到
// 最后一次完整保存的状态。
type FileStore struct{ dir string }

func NewFileStore(dir string) *FileStore { return &FileStore{dir: dir} }

func (s *FileStore) path(workflowID string) string {
	name := filepath.Base(workflowID)
	return filepath.Join(s.dir, "workflow-"+name+".json")
}

func (s *FileStore) Load(workflowID string) (*WorkflowSnapshot, error) {
	data, err := os.ReadFile(s.path(workflowID))
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var snap WorkflowSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return nil, err
	}
	return &snap, nil
}

func (s *FileStore) Save(snap *WorkflowSnapshot) error {
	if snap == nil {
		return errors.New("nil snapshot")
	}
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(snap, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.path(snap.WorkflowID) + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path(snap.WorkflowID))
}
