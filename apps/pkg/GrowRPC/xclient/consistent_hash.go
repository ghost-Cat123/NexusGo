package xclient

import (
	"hash/crc32"
	"sort"
	"strconv"
	"sync"
)

// Hash 无符号32位整数 [0, 2^32 - 1]
type Hash func(data []byte) uint32

type Map struct {
	// 加锁 保护一致性哈希环
	mu sync.RWMutex
	// 哈希表 允许替换成自定义哈希函数
	hash Hash
	// 虚拟节点的数量
	replicas int
	// 哈希环
	keys []int
	// 存储虚拟节点和真实节点的映射表 键是虚拟节点哈希值 值是真实节点的名称
	hashMap map[int]string
}

// New 构造函数 允许自定义虚拟节点倍数和hash函数
func New(replicas int, fn Hash) *Map {
	m := &Map{
		hash:     fn,
		replicas: replicas,
		hashMap:  make(map[int]string),
	}
	if m.hash == nil {
		// 默认采用ChecksumIEEE哈希算法
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// Add 添加真实节点 传入的是真实节点名称
func (m *Map) Add(keys ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// 遍历传入的节点名称
	for _, key := range keys {
		// 对于每个节点 创建replicas个虚拟节点
		for i := 0; i < m.replicas; i++ {
			// 计算虚拟节点的hash值，虚拟节点的名称添加编号前缀
			// 1是string类型 + key 就是"1" + "2" = "12" 字符串拼接
			hash := int(m.hash([]byte(strconv.Itoa(i) + key)))
			// 将hash值添加到环上
			m.keys = append(m.keys, hash)
			// 记录虚拟节点和真实节点的映射关系 一个真实节点 对应多个虚拟节点的hash值
			m.hashMap[hash] = key
		}
	}
	sort.Ints(m.keys)
}

// Get 选择去哪里找缓存 返回的是真实节点的名称
func (m *Map) Get(key string) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if len(m.keys) == 0 {
		return ""
	}
	// 计算要寻找节点的hash值
	hash := int(m.hash([]byte(key)))
	idx := sort.Search(len(m.keys), func(i int) bool {
		// 找一个比当前hash值大的节点 即顺时针旋转
		return m.keys[i] >= hash
	})

	// 在哈希表中寻找对应键的节点真实名称
	return m.hashMap[m.keys[idx%len(m.keys)]]
}
