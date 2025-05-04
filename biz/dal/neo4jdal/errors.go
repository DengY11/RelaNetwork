package neo4jdal

import "errors"

// ErrNotFound 表示在数据库中未找到请求的记录。
var ErrNotFound = errors.New("neo4jdal: record not found")
