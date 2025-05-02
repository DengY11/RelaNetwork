package cache

import "errors"

var (
	// ErrNotFound 表示缓存中未找到指定的键。
	// 这与数据库的 ErrNotFound 类似，用于区分是"没找到"还是其他错误。
	ErrNotFound = errors.New("cache: key not found")

	// ErrNilValue 表示缓存中存储的是一个代表"空"或"不存在"的特殊值。
	// 应用层在 Get 操作后检查到此错误时，应理解为数据确实不存在，而非缓存读取失败。
	ErrNilValue = errors.New("cache: stored nil value")
)
