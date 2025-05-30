package neo4jrepo

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/google/uuid"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j"
	"github.com/neo4j/neo4j-go-driver/v5/neo4j/dbtype"

	"labelwall/biz/dal/neo4jdal"
	network "labelwall/biz/model/relationship/network"
	"labelwall/pkg/cache" // 引入缓存包
)

// 保留与 TTL 无关的常量
const (
	getNodeRelationsCachePrefix      = "relation:list:ids:" // 缓存键前缀
	getNodeRelationsEmptyPlaceholder = "__EMPTY_REL_LIST__" // 空结果占位符
)

// neo4jRelationRepo 实现了 RelationRepository 接口
type neo4jRelationRepo struct {
	driver      neo4j.DriverWithContext
	relationDAL neo4jdal.RelationDAL
	cache       cache.RelationAndByteCache
	// 添加配置字段
	defaultTTL          time.Duration
	getNodeRelationsTTL time.Duration
	logger              *zap.Logger
}

// NewRelationRepository 创建一个新的 RelationRepository 实例
// 添加 TTL 参数 (秒)
func NewRelationRepository(
	driver neo4j.DriverWithContext,
	relationDAL neo4jdal.RelationDAL,
	cache cache.RelationAndByteCache,
	defaultTTLSeconds int,
	getNodeRelationsTTLSeconds int,
	logger *zap.Logger,
) RelationRepository {
	return &neo4jRelationRepo{
		driver:      driver,
		relationDAL: relationDAL,
		cache:       cache,
		// 将秒转换为 time.Duration
		defaultTTL:          time.Duration(defaultTTLSeconds) * time.Second,
		getNodeRelationsTTL: time.Duration(getNodeRelationsTTLSeconds) * time.Second,
		logger:              logger,
	}
}

// CreateRelation 创建一个新的关系
// 通常不直接影响基于 ID 的缓存
func (r *neo4jRelationRepo) CreateRelation(ctx context.Context, req *network.CreateRelationRequest) (*network.Relation, error) {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	// 1. 生成唯一业务 ID
	relationID := uuid.NewString()

	// 2. 构建关系属性 Map
	properties := map[string]any{
		"id":         relationID,
		"created_at": time.Now().UTC(),
		"updated_at": time.Now().UTC(),
	}
	if req.Label != nil {
		properties["label"] = *req.Label
	}
	if req.Properties != nil {
		for k, v := range req.Properties {
			if _, exists := properties[k]; !exists {
				properties[k] = v
			}
		}
	}

	// 3. 调用 DAL 层执行创建
	// ExecCreateRelation 期望返回创建的关系及其类型
	dbRel, err := r.relationDAL.ExecCreateRelation(ctx, session, req.Source, req.Target, req.Type, properties)
	if err != nil {
		return nil, fmt.Errorf("repo: 调用 DAL 创建关系失败: %w", err)
	}

	// 4. 映射结果
	// mapDbRelationshipToThriftRelation 需要源和目标 ID，这里直接用请求里的
	return mapDbRelationshipToThriftRelation(dbRel, req.Type, req.Source, req.Target), nil
}

// GetRelation 通过 ID 获取关系，应用 Read-Aside 缓存策略
func (r *neo4jRelationRepo) GetRelation(ctx context.Context, id string) (*network.Relation, error) {
	// 1. 尝试从缓存获取 (使用 r.cache)
	if r.cache != nil {
		cachedRel, err := r.cache.GetRelation(ctx, id)
		if err == nil {
			r.logger.Info("Repo: GetRelation cache hit", zap.String("id", id))
			return cachedRel, nil
		} else if errors.Is(err, cache.ErrNilValue) {
			r.logger.Info("Repo: GetRelation cache hit with nil value", zap.String("id", id))
			// 返回一个代表未找到的错误，与数据库行为一致
			return nil, fmt.Errorf("repo: relation %s not found (cached nil): %w", id, err)
		} else if !errors.Is(err, cache.ErrNotFound) {
			r.logger.Warn("Repo: 缓存获取关系失败", zap.String("id", id), zap.Error(err))
		}
	}

	// 2. 从数据库获取
	r.logger.Info("Repo: GetRelation cache miss, querying database", zap.String("id", id))
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	// ExecGetRelationByID 期望返回关系、类型字符串、源节点ID、目标节点ID
	dbRel, relTypeStr, sourceID, targetID, err := r.relationDAL.ExecGetRelationByID(ctx, session, id)
	if err != nil {
		if isNotFoundError(err) {
			// DB 未找到，不缓存空值（符合 redis_cache.go 里的 SetRelation 逻辑）
			return nil, err // 透传 Not Found
		}
		return nil, fmt.Errorf("repo: 调用 DAL 获取关系失败: %w", err)
	}

	// 3. 映射结果
	relType, ok := stringToRelationType(relTypeStr)
	if !ok {
		r.logger.Warn("Repo: 无法识别关系 的类型", zap.String("id", id), zap.String("relationType", relTypeStr))
		return nil, fmt.Errorf("repo: 无法识别的关系类型 %s", relTypeStr)
	}
	resultRel := mapDbRelationshipToThriftRelation(dbRel, relType, sourceID, targetID)

	// 4. 存入缓存 (使用 r.cache)
	if r.cache != nil && resultRel != nil {
		// 使用配置的 TTL
		setErr := r.cache.SetRelation(ctx, id, resultRel, r.defaultTTL)
		if setErr != nil {
			r.logger.Warn("Repo: 缓存设置关系失败", zap.String("id", id), zap.Error(setErr))
		}
	}

	return resultRel, nil
}

// UpdateRelation 更新关系属性，应用 Write Invalidation 缓存策略
func (r *neo4jRelationRepo) UpdateRelation(ctx context.Context, req *network.UpdateRelationRequest) (*network.Relation, error) {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	// 1. 构建更新 Map
	updates := map[string]any{
		"updated_at": time.Now().UTC(),
	}
	if req.Label != nil {
		updates["label"] = *req.Label
	}
	if req.Properties != nil {
		for k, v := range req.Properties {
			if k != "id" && k != "created_at" && k != "updated_at" {
				updates[k] = v
			}
		}
	}

	// 2. 调用 DAL 层执行更新
	// ExecUpdateRelation 返回更新后的关系、类型字符串、源和目标 ID
	// 注意：DAL 层不接受类型更新作为参数
	dbRel, relTypeStr, sourceID, targetID, err := r.relationDAL.ExecUpdateRelation(ctx, session, req.ID, updates)
	if err != nil {
		if isNotFoundError(err) {
			return nil, err // 透传 Not Found
		}
		return nil, fmt.Errorf("repo: 调用 DAL 更新关系失败: %w", err)
	}

	// 3. 映射结果
	relType, ok := stringToRelationType(relTypeStr)
	if !ok {
		r.logger.Warn("Repo: 更新后无法识别关系 的类型", zap.String("id", req.ID), zap.String("relationType", relTypeStr))
		return nil, fmt.Errorf("repo: 更新后无法识别的关系类型 %s", relTypeStr)
	}
	updatedRel := mapDbRelationshipToThriftRelation(dbRel, relType, sourceID, targetID)

	// 4. 使缓存失效 (使用 r.cache)
	if r.cache != nil {
		delErr := r.cache.DeleteRelation(ctx, req.ID)
		if delErr != nil && !errors.Is(delErr, cache.ErrNotFound) {
			r.logger.Warn("Repo: 缓存删除关系失败", zap.String("id", req.ID), zap.Error(delErr))
		}
	}

	return updatedRel, nil
}

// DeleteRelation 删除关系，应用 Write Invalidation 缓存策略
func (r *neo4jRelationRepo) DeleteRelation(ctx context.Context, id string) error {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeWrite})
	defer session.Close(ctx)

	// 1. 调用 DAL 层执行删除
	err := r.relationDAL.ExecDeleteRelation(ctx, session, id)
	if err != nil {
		if isNotFoundError(err) {
			// DB 中不存在，仍然尝试删除缓存
		} else {
			// 其他数据库错误
			return fmt.Errorf("repo: 调用 DAL 删除关系失败: %w", err)
		}
	}

	// 2. 使缓存失效 (使用 r.cache)
	if r.cache != nil {
		delErr := r.cache.DeleteRelation(ctx, id)
		if delErr != nil && !errors.Is(delErr, cache.ErrNotFound) {
			r.logger.Warn("Repo: 缓存删除关系失败", zap.String("id", id), zap.Error(delErr))
		}
	}

	// 如果原始错误是 NotFound，则透传它
	if isNotFoundError(err) {
		return err
	}

	return nil // DB 删除成功（或本来就不存在）且尝试删除缓存后返回 nil
}

// GetNodeRelations 获取节点的关系列表 (带缓存)
func (r *neo4jRelationRepo) GetNodeRelations(ctx context.Context, req *network.GetNodeRelationsRequest) ([]*network.Relation, int32, error) {
	// 1. 处理参数 (与缓存键生成相关)
	var relTypesStr []string
	if req.IsSetTypes() && len(req.Types) > 0 {
		relTypesStr = make([]string, 0, len(req.Types))
		for _, rt := range req.Types {
			relTypesStr = append(relTypesStr, rt.String())
		}
	}
	outgoing := true
	if req.IsSetOutgoing() {
		outgoing = *req.Outgoing
	}
	incoming := true
	if req.IsSetIncoming() {
		incoming = *req.Incoming
	}
	var limit, offset int64
	if req.IsSetLimit() {
		limit = int64(*req.Limit)
	} else {
		limit = 10
	}
	if req.IsSetOffset() {
		offset = int64(*req.Offset)
	} else {
		offset = 0
	}

	// 2. 检查缓存是否可用
	if r.cache == nil {
		r.logger.Warn("Repo: GetNodeRelations cache not initialized, skipping cache.")
		return r.getNodeRelationsDirect(ctx, req, relTypesStr, outgoing, incoming, limit, offset)
	}

	// 3. 生成缓存键
	cacheKey := generateGetNodeRelationsCacheKey(req, relTypesStr, outgoing, incoming, limit, offset)

	// 4. 尝试从缓存获取 (使用通用的 Get)
	cachedData, err := r.cache.Get(ctx, cacheKey)
	if err == nil { // 缓存命中
		// 4.1 检查空标记
		if bytes.Equal(cachedData, []byte(getNodeRelationsEmptyPlaceholder)) {
			r.logger.Info("Repo: GetNodeRelations cache hit empty placeholder", zap.String("cacheKey", cacheKey))
			return []*network.Relation{}, 0, nil
		}

		// 4.2 解析缓存的 ID 列表和总数
		var cachedValue getNodeRelationsCacheValue
		if err := json.NewDecoder(bytes.NewReader(cachedData)).Decode(&cachedValue); err == nil {
			r.logger.Info("Repo: GetNodeRelations cache hit, fetching details", zap.String("cacheKey", cacheKey))
			resultRelations := make([]*network.Relation, 0, len(cachedValue.RelationIDs))
			failedFetches := 0

			// 4.3 使用 GetRelation 获取关系详情
			for _, relID := range cachedValue.RelationIDs {
				relation, getErr := r.GetRelation(ctx, relID)
				if getErr != nil {
					failedFetches++
					if errors.Is(getErr, cache.ErrNotFound) || isNotFoundError(getErr) || errors.Is(getErr, cache.ErrNilValue) {
						r.logger.Warn("Repo: GetNodeRelations cache hit, but GetRelation couldn't find relation", zap.String("relationID", relID))
					} else {
						r.logger.Error("Repo: GetNodeRelations cache hit, but GetRelation failed", zap.String("relationID", relID), zap.Error(getErr))
					}
					continue // 跳过获取失败的关系
				}
				if relation != nil {
					resultRelations = append(resultRelations, relation)
				}
			}
			if failedFetches > 0 {
				r.logger.Warn("Repo: GetNodeRelations cache hit, but relations failed to fetch", zap.Int("failedFetches", failedFetches), zap.String("cacheKey", cacheKey))
			}
			return resultRelations, cachedValue.Total, nil
		}
		// 缓存数据解析失败，当作未命中
		r.logger.Error("Repo: GetNodeRelations cache data decode failed", zap.String("cacheKey", cacheKey), zap.Error(err))
	} else if !errors.Is(err, cache.ErrNotFound) {
		r.logger.Error("Repo: GetNodeRelations cache get failed", zap.String("cacheKey", cacheKey), zap.Error(err))
	} else {
		r.logger.Info("Repo: GetNodeRelations cache miss", zap.String("cacheKey", cacheKey))
	}

	// 5. 缓存未命中或出错，直接查询数据库
	resultRelations, total, dbRels, err := r.getNodeRelationsDirectAndRaw(ctx, req, relTypesStr, outgoing, incoming, limit, offset)
	if err != nil {
		return nil, 0, err // 直接返回数据库错误
	}

	// 6. 缓存结果
	if err == nil {
		if len(dbRels) == 0 {
			// 6.1 缓存空标记 (使用 cache 包的 NilValueTTL 或单独配置)
			// 注意：之前的 getNodeRelationsEmptyTTL 常量已被移除
			// 依赖 cache 包内部的 NilValueTTL 或需要从配置传入
			setErr := r.cache.Set(ctx, cacheKey, []byte(getNodeRelationsEmptyPlaceholder), cache.NilValueTTL) // 假设使用 cache.NilValueTTL
			if setErr != nil {
				r.logger.Error("Repo: GetNodeRelations cache set empty placeholder failed", zap.String("cacheKey", cacheKey), zap.Error(setErr))
			} else {
				r.logger.Info("Repo: GetNodeRelations set empty placeholder to cache", zap.String("cacheKey", cacheKey))
			}
		} else {
			// 6.2 提取 ID 并缓存 (使用配置的 TTL)
			relationIDs := make([]string, 0, len(dbRels))
			for _, dbRel := range dbRels {
				if relID := getStringProp(dbRel.Props, "id", ""); relID != "" {
					relationIDs = append(relationIDs, relID)
				} else {
					r.logger.Error("Repo: GetNodeRelations DB result relation missing 'id'", zap.String("elementId", dbRel.ElementId))
					goto SkipCache // 无法提取所有 ID，跳过缓存
				}
			}

			cacheValue := getNodeRelationsCacheValue{
				RelationIDs: relationIDs,
				Total:       int32(total),
			}
			var buffer bytes.Buffer
			if encErr := json.NewEncoder(&buffer).Encode(cacheValue); encErr == nil {
				// 使用配置的 getNodeRelationsTTL
				setErr := r.cache.Set(ctx, cacheKey, buffer.Bytes(), r.getNodeRelationsTTL)
				if setErr != nil {
					r.logger.Error("Repo: GetNodeRelations cache set failed", zap.String("cacheKey", cacheKey), zap.Error(setErr))
				} else {
					r.logger.Info("Repo: GetNodeRelations set data to cache", zap.String("cacheKey", cacheKey))
				}
			} else {
				r.logger.Error("Repo: GetNodeRelations cache value encode failed", zap.String("cacheKey", cacheKey), zap.Error(encErr))
			}
		}
	}

SkipCache:
	// 7. 返回从数据库获取并映射的结果
	return resultRelations, total, nil
}

// getNodeRelationsDirectAndRaw 封装了直接的数据库查询和映射逻辑
func (r *neo4jRelationRepo) getNodeRelationsDirectAndRaw(ctx context.Context, req *network.GetNodeRelationsRequest, relTypesStr []string, outgoing, incoming bool, limit, offset int64) (
	resultRelations []*network.Relation,
	total int32,
	dbRels []dbtype.Relationship,
	err error,
) {
	session := r.driver.NewSession(ctx, neo4j.SessionConfig{AccessMode: neo4j.AccessModeRead})
	defer session.Close(ctx)

	var dbTotal int64 // DAL 返回 int64
	dbRels, relTypeStrs, sourceIDs, targetIDs, dbTotal, err := r.relationDAL.ExecGetNodeRelations(ctx, session, req.NodeID, relTypesStr, outgoing, incoming, limit, offset)
	if err != nil {
		err = fmt.Errorf("repo: 调用 DAL 获取节点关系失败: %w", err)
		return
	}
	total = int32(dbTotal) // 类型转换

	resultRelations = make([]*network.Relation, 0, len(dbRels))
	for i, dbRel := range dbRels {
		relTypeStr := relTypeStrs[i]
		sourceID := sourceIDs[i]
		targetID := targetIDs[i]

		relType, ok := stringToRelationType(relTypeStr)
		if !ok {
			relID := getStringProp(dbRel.Props, "id", "[未知ID]")
			r.logger.Warn("Repo: GetNodeRelations 中无法识别关系 的类型", zap.String("id", relID), zap.String("relationType", relTypeStr))
			continue
		}

		thriftRelation := mapDbRelationshipToThriftRelation(dbRel, relType, sourceID, targetID)
		if thriftRelation != nil {
			resultRelations = append(resultRelations, thriftRelation)
		}
	}

	return // 返回映射结果、总数、原始关系和 nil 错误
}

// getNodeRelationsDirect (旧版，仅用于在缓存未初始化时调用)
func (r *neo4jRelationRepo) getNodeRelationsDirect(ctx context.Context, req *network.GetNodeRelationsRequest, relTypesStr []string, outgoing, incoming bool, limit, offset int64) ([]*network.Relation, int32, error) {
	rr, total, _, err := r.getNodeRelationsDirectAndRaw(ctx, req, relTypesStr, outgoing, incoming, limit, offset)
	return rr, total, err
}

// --- GetNodeRelations Caching --- //

// getNodeRelationsCacheValue 定义了 GetNodeRelations 结果缓存的结构
type getNodeRelationsCacheValue struct {
	RelationIDs []string `json:"relation_ids"`
	Total       int32    `json:"total"`
}

// generateGetNodeRelationsCacheKey 生成 GetNodeRelations 的缓存键
func generateGetNodeRelationsCacheKey(req *network.GetNodeRelationsRequest, relTypesStr []string, outgoing, incoming bool, limit, offset int64) string {
	// 对关系类型字符串进行排序，确保顺序无关性
	sortedTypes := make([]string, len(relTypesStr))
	copy(sortedTypes, relTypesStr)
	sort.Strings(sortedTypes)
	typesKeyPart := strings.Join(sortedTypes, ",")

	// 对类型部分进行哈希
	hasher := sha1.New()
	hasher.Write([]byte(typesKeyPart))
	typesHash := hex.EncodeToString(hasher.Sum(nil))

	// 方向的规范表示
	direction := "any"
	if outgoing && !incoming {
		direction = "out"
	} else if !outgoing && incoming {
		direction = "in"
	} // else if !outgoing && !incoming? -> DAL 应该处理，这里当作 "any"

	// 格式: prefix:nodeID:direction:typesHash:limit:offset
	return fmt.Sprintf("%s%s:%s:%s:%d:%d",
		getNodeRelationsCachePrefix, req.NodeID, direction, typesHash, limit, offset)
}
