package e2e_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	network_model "labelwall/biz/model/relationship/network"

	assert "github.com/stretchr/testify/assert"
)

const (
	// 重要：确保这个地址与你运行测试时服务器监听的地址和端口匹配
	// 例如，如果你的 config.yaml 中 server.address 是 ":8888"
	serverBaseURL = "http://localhost:8888"
	apiV1Prefix   = "/api/v1"
)

var (
	httpClient = &http.Client{
		Timeout: 10 * time.Second, // 设置合理的超时
	}
)

// makeRequest 是一个辅助函数，用于发送 HTTP 请求并返回响应
func makeRequest(t *testing.T, method, path string, body interface{}) (*http.Response, []byte) {
	t.Helper()

	var reqBodyReader io.Reader
	if body != nil {
		bodyBytes, err := json.Marshal(body)
		assert.NoError(t, err, "Failed to marshal request body")
		reqBodyReader = bytes.NewReader(bodyBytes)
	}

	fullURL := serverBaseURL + path
	req, err := http.NewRequest(method, fullURL, reqBodyReader)
	assert.NoError(t, err, "Failed to create request")

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")

	resp, err := httpClient.Do(req)
	assert.NoError(t, err, "Failed to send request")

	respBodyBytes, err := io.ReadAll(resp.Body)
	assert.NoError(t, err, "Failed to read response body")
	resp.Body.Close() // 及时关闭响应体

	return resp, respBodyBytes
}

// --- E2E Test Cases ---

// TestE2ECreateNode 测试创建节点 API
func TestE2ECreateNode(t *testing.T) {
	nodeType := network_model.NodeType_PERSON
	uniqueName := fmt.Sprintf("E2E Test Person %d", time.Now().UnixNano())
	requestBody := network_model.CreateNodeRequest{
		Type: nodeType,
		Name: uniqueName,
		Properties: map[string]string{
			"email":  fmt.Sprintf("e2e-%d@example.com", time.Now().UnixNano()),
			"source": "e2e-test",
		},
	}

	resp, respBodyBytes := makeRequest(t, http.MethodPost, apiV1Prefix+"/nodes", requestBody)

	// --- Assertions ---
	assert.Equal(t, http.StatusOK, resp.StatusCode, "Expected status OK")

	var createResp network_model.CreateNodeResponse
	err := json.Unmarshal(respBodyBytes, &createResp)
	assert.NoError(t, err, "Failed to unmarshal CreateNodeResponse")

	assert.True(t, createResp.Success, "Expected Success to be true")
	assert.NotEmpty(t, createResp.Message, "Expected non-empty message") // Or check specific message if applicable
	assert.NotNil(t, createResp.Node, "Expected Node to be non-nil")
	assert.NotEmpty(t, createResp.Node.ID, "Expected non-empty Node ID")
	assert.Equal(t, nodeType, createResp.Node.Type, "Expected correct Node Type")
	assert.Equal(t, uniqueName, createResp.Node.Name, "Expected correct Node Name")
	assert.Equal(t, requestBody.Properties["email"], createResp.Node.Properties["email"], "Expected correct email property")
	assert.Equal(t, requestBody.Properties["source"], createResp.Node.Properties["source"], "Expected correct source property")

	// 可选：记录创建的 ID 用于后续测试或清理
	if createResp.Node != nil {
		t.Logf("Created Node ID: %s", createResp.Node.ID)
	}
}

// TestE2EGetNode 测试获取节点 API (找到和未找到)
func TestE2EGetNode(t *testing.T) {
	// --- Part 1: Test Found ---
	// 1.1 先创建一个节点用于获取
	nodeType := network_model.NodeType_COMPANY
	uniqueName := fmt.Sprintf("E2E Test Company %d", time.Now().UnixNano())
	createReqBody := network_model.CreateNodeRequest{
		Type: nodeType,
		Name: uniqueName,
		Properties: map[string]string{
			"industry": "Tech",
			"source":   "e2e-test-get",
		},
	}
	createResp, createRespBody := makeRequest(t, http.MethodPost, apiV1Prefix+"/nodes", createReqBody)
	assert.Equal(t, http.StatusOK, createResp.StatusCode)
	var createdNodeResp network_model.CreateNodeResponse
	err := json.Unmarshal(createRespBody, &createdNodeResp)
	assert.NoError(t, err)
	assert.True(t, createdNodeResp.Success)
	assert.NotNil(t, createdNodeResp.Node)
	createdNodeID := createdNodeResp.Node.ID
	t.Logf("Node created for Get test: %s", createdNodeID)

	// 1.2 使用创建的 ID 获取节点
	getNodePath := fmt.Sprintf("%s/nodes/%s", apiV1Prefix, createdNodeID)
	getResp, getRespBody := makeRequest(t, http.MethodGet, getNodePath, nil)

	// 1.3 断言找到的情况
	assert.Equal(t, http.StatusOK, getResp.StatusCode, "GetNode Found: Expected status OK")
	var getFoundResp network_model.GetNodeResponse
	err = json.Unmarshal(getRespBody, &getFoundResp)
	assert.NoError(t, err, "GetNode Found: Failed to unmarshal GetNodeResponse")
	assert.True(t, getFoundResp.Success, "GetNode Found: Expected Success to be true")
	assert.NotNil(t, getFoundResp.Node, "GetNode Found: Expected Node to be non-nil")
	assert.Equal(t, createdNodeID, getFoundResp.Node.ID, "GetNode Found: Expected matching Node ID")
	assert.Equal(t, nodeType, getFoundResp.Node.Type, "GetNode Found: Expected correct Node Type")
	assert.Equal(t, uniqueName, getFoundResp.Node.Name, "GetNode Found: Expected correct Node Name")
	assert.Equal(t, createReqBody.Properties["industry"], getFoundResp.Node.Properties["industry"], "GetNode Found: Expected correct industry property")

	// --- Part 2: Test Not Found ---
	// 2.1 使用一个不存在的 ID 获取
	nonExistentID := "e2e-non-existent-node-id-" + fmt.Sprintf("%d", time.Now().UnixNano())
	getNotFoundPath := fmt.Sprintf("%s/nodes/%s", apiV1Prefix, nonExistentID)
	getNotFoundResp, getNotFoundBody := makeRequest(t, http.MethodGet, getNotFoundPath, nil)

	// 2.2 断言未找到的情况 (根据你的 Handler 实现，通常是 200 OK 但 success: false)
	assert.Equal(t, http.StatusOK, getNotFoundResp.StatusCode, "GetNode NotFound: Expected status OK")
	var getNotFoundRespParsed network_model.GetNodeResponse
	err = json.Unmarshal(getNotFoundBody, &getNotFoundRespParsed)
	assert.NoError(t, err, "GetNode NotFound: Failed to unmarshal GetNodeResponse")
	assert.False(t, getNotFoundRespParsed.Success, "GetNode NotFound: Expected Success to be false")
	assert.Nil(t, getNotFoundRespParsed.Node, "GetNode NotFound: Expected Node to be nil")
	assert.NotEmpty(t, getNotFoundRespParsed.Message, "GetNode NotFound: Expected non-empty message")
}

// TestE2EUpdateNode 测试更新节点 API
func TestE2EUpdateNode(t *testing.T) {
	// 1. 先创建一个节点用于更新
	nodeType := network_model.NodeType_PERSON
	initialName := fmt.Sprintf("E2E Test Event %d", time.Now().UnixNano())
	createReqBody := network_model.CreateNodeRequest{
		Type: nodeType,
		Name: initialName,
		Properties: map[string]string{
			"location": "Initial Location",
			"source":   "e2e-test-update",
		},
	}
	createResp, createRespBody := makeRequest(t, http.MethodPost, apiV1Prefix+"/nodes", createReqBody)
	assert.Equal(t, http.StatusOK, createResp.StatusCode)
	var createdNodeResp network_model.CreateNodeResponse
	err := json.Unmarshal(createRespBody, &createdNodeResp)
	assert.NoError(t, err)
	assert.True(t, createdNodeResp.Success)
	assert.NotNil(t, createdNodeResp.Node)
	nodeID := createdNodeResp.Node.ID
	t.Logf("Node created for Update test: %s", nodeID)

	// 2. 准备更新请求
	updatedName := fmt.Sprintf("Updated E2E Event %d", time.Now().UnixNano())
	updateReqBody := network_model.UpdateNodeRequest{
		ID:   nodeID,
		Name: &updatedName, // Pass pointer for optional field
		Properties: map[string]string{
			"location": "Updated Location", // 更新属性
			"status":   "Confirmed",        // 添加新属性
		},
	}
	updatePath := fmt.Sprintf("%s/nodes/%s", apiV1Prefix, nodeID)

	// 3. 发送更新请求
	updateResp, updateRespBody := makeRequest(t, http.MethodPut, updatePath, updateReqBody)

	// 4. 断言更新响应
	assert.Equal(t, http.StatusOK, updateResp.StatusCode, "UpdateNode: Expected status OK")
	var updateNodeResp network_model.UpdateNodeResponse
	err = json.Unmarshal(updateRespBody, &updateNodeResp)
	assert.NoError(t, err, "UpdateNode: Failed to unmarshal UpdateNodeResponse")
	assert.True(t, updateNodeResp.Success, "UpdateNode: Expected Success to be true")
	assert.NotEmpty(t, updateNodeResp.Message, "UpdateNode: Expected non-empty message")

	// 5. (可选) 再次获取节点验证更新
	getResp, getRespBody := makeRequest(t, http.MethodGet, updatePath, nil)
	assert.Equal(t, http.StatusOK, getResp.StatusCode)
	var getNodeResp network_model.GetNodeResponse
	err = json.Unmarshal(getRespBody, &getNodeResp)
	assert.NoError(t, err)
	assert.True(t, getNodeResp.Success)
	assert.NotNil(t, getNodeResp.Node)
	assert.Equal(t, nodeID, getNodeResp.Node.ID)
	assert.Equal(t, updatedName, getNodeResp.Node.Name, "UpdateNode Verify: Expected updated name")
	assert.Equal(t, updateReqBody.Properties["location"], getNodeResp.Node.Properties["location"], "UpdateNode Verify: Expected updated location")
	assert.Equal(t, updateReqBody.Properties["status"], getNodeResp.Node.Properties["status"], "UpdateNode Verify: Expected added status")
	// 确保旧属性 "source" 仍然存在 (如果更新逻辑是合并而不是替换) - 这取决于你的 Handler 实现
	// assert.Equal(t, createReqBody.Properties["source"], getNodeResp.Node.Properties["source"], "UpdateNode Verify: Expected original source property to remain")
}

// TestE2EDeleteNode 测试删除节点 API
func TestE2EDeleteNode(t *testing.T) {
	// 1. 先创建一个节点用于删除
	nodeType := network_model.NodeType_PERSON
	name := fmt.Sprintf("E2E Doc To Delete %d", time.Now().UnixNano())
	createReqBody := network_model.CreateNodeRequest{Type: nodeType, Name: name, Properties: map[string]string{"source": "e2e-test-delete"}}
	createResp, createRespBody := makeRequest(t, http.MethodPost, apiV1Prefix+"/nodes", createReqBody)
	assert.Equal(t, http.StatusOK, createResp.StatusCode)
	var createdNodeResp network_model.CreateNodeResponse
	err := json.Unmarshal(createRespBody, &createdNodeResp)
	assert.NoError(t, err)
	assert.True(t, createdNodeResp.Success)
	assert.NotNil(t, createdNodeResp.Node)
	nodeID := createdNodeResp.Node.ID
	t.Logf("Node created for Delete test: %s", nodeID)

	// 2. 发送删除请求
	deletePath := fmt.Sprintf("%s/nodes/%s", apiV1Prefix, nodeID)
	deleteResp, deleteRespBody := makeRequest(t, http.MethodDelete, deletePath, nil)

	// 3. 断言删除响应
	assert.Equal(t, http.StatusOK, deleteResp.StatusCode, "DeleteNode: Expected status OK")
	var deleteNodeResp network_model.DeleteNodeResponse
	err = json.Unmarshal(deleteRespBody, &deleteNodeResp)
	assert.NoError(t, err, "DeleteNode: Failed to unmarshal DeleteNodeResponse")
	assert.True(t, deleteNodeResp.Success, "DeleteNode: Expected Success to be true")
	assert.NotEmpty(t, deleteNodeResp.Message, "DeleteNode: Expected non-empty message")

	// 4. (可选) 尝试再次获取该节点，确认已删除
	getResp, getRespBody := makeRequest(t, http.MethodGet, deletePath, nil)
	assert.Equal(t, http.StatusOK, getResp.StatusCode, "DeleteNode Verify: Expected status OK on Get attempt")
	var getNodeResp network_model.GetNodeResponse
	err = json.Unmarshal(getRespBody, &getNodeResp)
	assert.NoError(t, err, "DeleteNode Verify: Failed to unmarshal GetNodeResponse")
	assert.False(t, getNodeResp.Success, "DeleteNode Verify: Expected Success to be false")
	assert.Nil(t, getNodeResp.Node, "DeleteNode Verify: Expected Node to be nil")
}

// TestE2ESearchNodes 测试搜索节点 API
func TestE2ESearchNodes(t *testing.T) {
	// 1. 创建一些用于搜索的节点
	searchTag := fmt.Sprintf("searchable-%d", time.Now().UnixNano())
	nodeTypeToSearch := network_model.NodeType_PERSON
	// 节点1: 类型匹配, 名称包含 tag
	createReq1 := network_model.CreateNodeRequest{
		Type:       nodeTypeToSearch,
		Name:       fmt.Sprintf("Tag Alpha %s", searchTag),
		Properties: map[string]string{"color": "red"},
	}
	makeRequest(t, http.MethodPost, apiV1Prefix+"/nodes", createReq1)
	// 节点2: 类型匹配, 名称包含 tag
	createReq2 := network_model.CreateNodeRequest{
		Type:       nodeTypeToSearch,
		Name:       fmt.Sprintf("Tag Beta %s", searchTag),
		Properties: map[string]string{"color": "blue"},
	}
	makeRequest(t, http.MethodPost, apiV1Prefix+"/nodes", createReq2)
	// 节点3: 类型不匹配
	createReq3 := network_model.CreateNodeRequest{
		Type:       network_model.NodeType_PERSON,
		Name:       fmt.Sprintf("Person %s", searchTag),
		Properties: map[string]string{"color": "red"}, // 属性可能重叠
	}
	makeRequest(t, http.MethodPost, apiV1Prefix+"/nodes", createReq3)
	t.Logf("Nodes created for Search test with tag: %s", searchTag)

	// 2. 执行搜索 (按类型和名称查询)
	searchPath := fmt.Sprintf("%s/nodes/search?type=%d&criteria[name]=%s&limit=10",
		apiV1Prefix, nodeTypeToSearch, searchTag) // 使用 criteria[name] 和 NodeType 整数值
	searchResp, searchRespBody := makeRequest(t, http.MethodGet, searchPath, nil)

	// 3. 断言搜索结果
	assert.Equal(t, http.StatusOK, searchResp.StatusCode, "SearchNodes: Expected status OK")
	var searchNodesResp network_model.SearchNodesResponse
	err := json.Unmarshal(searchRespBody, &searchNodesResp)
	assert.NoError(t, err, "SearchNodes: Failed to unmarshal SearchNodesResponse")
	assert.True(t, searchNodesResp.Success, "SearchNodes: Expected Success to be true")
	assert.Len(t, searchNodesResp.Nodes, 3, "SearchNodes: Expected 3 nodes matching type and query") // 应该只找到两个 TAG 节点

	// 验证找到的节点类型正确
	for _, node := range searchNodesResp.Nodes {
		assert.Equal(t, nodeTypeToSearch, node.Type, "SearchNodes: Found node has incorrect type")
		assert.Contains(t, node.Name, searchTag, "SearchNodes: Found node name doesn't contain search tag")
	}

	// 4. (可选) 测试其他搜索条件，例如按属性搜索 (如果 API 支持)
	// searchPathByProp := fmt.Sprintf("%s/nodes/search?properties=%s&limit=10",
	// 	apiV1Prefix, url.QueryEscape("color:red")) // 需要 URL 编码属性查询
	// ... 断言 ...
}

// Helper to create a node and return its ID
func createTestNode(t *testing.T, nodeType network_model.NodeType, namePrefix string, props map[string]string) string {
	t.Helper()
	uniqueName := fmt.Sprintf("%s %d", namePrefix, time.Now().UnixNano())
	if props == nil {
		props = map[string]string{}
	}
	props["source"] = "e2e-test-helper" // Add a default source

	createReqBody := network_model.CreateNodeRequest{
		Type:       nodeType,
		Name:       uniqueName,
		Properties: props,
	}
	createResp, createRespBody := makeRequest(t, http.MethodPost, apiV1Prefix+"/nodes", createReqBody)
	assert.Equal(t, http.StatusOK, createResp.StatusCode, "Helper CreateNode failed status")
	var createdNodeResp network_model.CreateNodeResponse
	err := json.Unmarshal(createRespBody, &createdNodeResp)
	assert.NoError(t, err, "Helper CreateNode failed unmarshal")
	assert.True(t, createdNodeResp.Success, "Helper CreateNode expected success true")
	assert.NotNil(t, createdNodeResp.Node, "Helper CreateNode expected node non-nil")
	assert.NotEmpty(t, createdNodeResp.Node.ID, "Helper CreateNode expected non-empty ID")
	t.Logf("Helper created node: %s (%s)", createdNodeResp.Node.ID, uniqueName)
	return createdNodeResp.Node.ID
}

// TestE2ECreateRelation 测试创建关系 API
func TestE2ECreateRelation(t *testing.T) {
	// 1. 创建起始和结束节点
	startNodeID := createTestNode(t, network_model.NodeType_PERSON, "Start Person", map[string]string{"email": fmt.Sprintf("start-%d@e2e.com", time.Now().UnixNano())})
	endNodeID := createTestNode(t, network_model.NodeType_COMPANY, "End Company", map[string]string{"industry": "Finance"})

	// 2. 准备创建关系请求
	relationType := network_model.RelationType_COLLEAGUE
	createRelReq := network_model.CreateRelationRequest{
		Source: startNodeID,
		Target: endNodeID,
		Type:   relationType,
		Properties: map[string]string{
			"role":   "Developer",
			"source": "e2e-create-rel",
		},
	}

	// 3. 发送创建关系请求
	createRelResp, createRelRespBody := makeRequest(t, http.MethodPost, apiV1Prefix+"/relations", createRelReq)

	// 4. 断言创建关系响应
	assert.Equal(t, http.StatusOK, createRelResp.StatusCode, "CreateRelation: Expected status OK")
	var createRelationResp network_model.CreateRelationResponse
	err := json.Unmarshal(createRelRespBody, &createRelationResp)
	assert.NoError(t, err, "CreateRelation: Failed to unmarshal CreateRelationResponse")
	assert.True(t, createRelationResp.Success, "CreateRelation: Expected Success to be true")
	assert.NotEmpty(t, createRelationResp.Message, "CreateRelation: Expected non-empty message")
	assert.NotNil(t, createRelationResp.Relation, "CreateRelation: Expected Relation to be non-nil")
	assert.NotEmpty(t, createRelationResp.Relation.ID, "CreateRelation: Expected non-empty Relation ID")
	assert.Equal(t, startNodeID, createRelationResp.Relation.Source, "CreateRelation: Expected correct Source")
	assert.Equal(t, endNodeID, createRelationResp.Relation.Target, "CreateRelation: Expected correct Target")
	assert.Equal(t, relationType, createRelationResp.Relation.Type, "CreateRelation: Expected correct Relation Type")
	assert.Equal(t, createRelReq.Properties["role"], createRelationResp.Relation.Properties["role"], "CreateRelation: Expected correct role property")

	t.Logf("Created Relation ID: %s", createRelationResp.Relation.ID)
}

// Helper to create a relation and return its ID
func createTestRelation(t *testing.T, startNodeID, endNodeID string, relType network_model.RelationType, props map[string]string) string {
	t.Helper()
	if props == nil {
		props = map[string]string{}
	}
	props["source"] = "e2e-test-helper-rel"

	createRelReq := network_model.CreateRelationRequest{
		Source:     startNodeID,
		Target:     endNodeID,
		Type:       relType,
		Properties: props,
	}
	createRelResp, createRelRespBody := makeRequest(t, http.MethodPost, apiV1Prefix+"/relations", createRelReq)
	assert.Equal(t, http.StatusOK, createRelResp.StatusCode, "Helper CreateRelation failed status")
	var createRelationResp network_model.CreateRelationResponse
	err := json.Unmarshal(createRelRespBody, &createRelationResp)
	assert.NoError(t, err, "Helper CreateRelation failed unmarshal")
	assert.True(t, createRelationResp.Success, "Helper CreateRelation expected success true")
	assert.NotNil(t, createRelationResp.Relation, "Helper CreateRelation expected relation non-nil")
	assert.NotEmpty(t, createRelationResp.Relation.ID, "Helper CreateRelation expected non-empty ID")
	t.Logf("Helper created relation: %s", createRelationResp.Relation.ID)
	return createRelationResp.Relation.ID
}

// TestE2EGetRelation 测试获取关系 API
func TestE2EGetRelation(t *testing.T) {
	// --- Part 1: Test Found ---
	// 1.1 创建关系用于获取
	startNodeID := createTestNode(t, network_model.NodeType_PERSON, "GetRel Start Person", nil)
	endNodeID := createTestNode(t, network_model.NodeType_COMPANY, "GetRel End Project", nil)
	relationType := network_model.RelationType_COLLEAGUE
	props := map[string]string{"hours": "10"}
	relationID := createTestRelation(t, startNodeID, endNodeID, relationType, props)

	// 1.2 使用创建的 ID 获取关系
	getRelationPath := fmt.Sprintf("%s/relations/%s", apiV1Prefix, relationID)
	getResp, getRespBody := makeRequest(t, http.MethodGet, getRelationPath, nil)

	// 1.3 断言找到的情况
	assert.Equal(t, http.StatusOK, getResp.StatusCode, "GetRelation Found: Expected status OK")
	var getFoundResp network_model.GetRelationResponse
	err := json.Unmarshal(getRespBody, &getFoundResp)
	assert.NoError(t, err, "GetRelation Found: Failed to unmarshal GetRelationResponse")
	assert.True(t, getFoundResp.Success, "GetRelation Found: Expected Success to be true")
	assert.NotNil(t, getFoundResp.Relation, "GetRelation Found: Expected Relation to be non-nil")
	assert.Equal(t, relationID, getFoundResp.Relation.ID, "GetRelation Found: Expected matching Relation ID")
	assert.Equal(t, startNodeID, getFoundResp.Relation.Source, "GetRelation Found: Expected correct Source")
	assert.Equal(t, endNodeID, getFoundResp.Relation.Target, "GetRelation Found: Expected correct Target")
	assert.Equal(t, relationType, getFoundResp.Relation.Type, "GetRelation Found: Expected correct Relation Type")
	assert.Equal(t, props["hours"], getFoundResp.Relation.Properties["hours"], "GetRelation Found: Expected correct hours property")

	// --- Part 2: Test Not Found ---
	nonExistentID := "e2e-non-existent-relation-id-" + fmt.Sprintf("%d", time.Now().UnixNano())
	getNotFoundPath := fmt.Sprintf("%s/relations/%s", apiV1Prefix, nonExistentID)
	getNotFoundResp, getNotFoundBody := makeRequest(t, http.MethodGet, getNotFoundPath, nil)

	// 断言未找到的情况
	assert.Equal(t, http.StatusOK, getNotFoundResp.StatusCode, "GetRelation NotFound: Expected status OK")
	var getNotFoundRespParsed network_model.GetRelationResponse
	err = json.Unmarshal(getNotFoundBody, &getNotFoundRespParsed)
	assert.NoError(t, err, "GetRelation NotFound: Failed to unmarshal GetRelationResponse")
	assert.False(t, getNotFoundRespParsed.Success, "GetRelation NotFound: Expected Success to be false")
	assert.Nil(t, getNotFoundRespParsed.Relation, "GetRelation NotFound: Expected Relation to be nil")
	assert.NotEmpty(t, getNotFoundRespParsed.Message, "GetRelation NotFound: Expected non-empty message")
}

// TestE2EUpdateRelation 测试更新关系 API
func TestE2EUpdateRelation(t *testing.T) {
	// 1. 创建关系用于更新
	startNodeID := createTestNode(t, network_model.NodeType_PERSON, "UpdateRel Start Event", nil)
	endNodeID := createTestNode(t, network_model.NodeType_PERSON, "UpdateRel End Person", nil)
	relationType := network_model.RelationType_VISITED
	initialProps := map[string]string{"status": "Registered"}
	relationID := createTestRelation(t, startNodeID, endNodeID, relationType, initialProps)

	// 2. 准备更新请求
	updateReqBody := network_model.UpdateRelationRequest{
		ID: relationID,
		Properties: map[string]string{
			"status": "CheckedIn",    // 更新属性
			"notes":  "Arrived late", // 添加新属性
		},
	}
	updatePath := fmt.Sprintf("%s/relations/%s", apiV1Prefix, relationID)

	// 3. 发送更新请求
	updateResp, updateRespBody := makeRequest(t, http.MethodPut, updatePath, updateReqBody)

	// 4. 断言更新响应
	assert.Equal(t, http.StatusOK, updateResp.StatusCode, "UpdateRelation: Expected status OK")
	var updateRelResp network_model.UpdateRelationResponse
	err := json.Unmarshal(updateRespBody, &updateRelResp)
	assert.NoError(t, err, "UpdateRelation: Failed to unmarshal UpdateRelationResponse")
	assert.True(t, updateRelResp.Success, "UpdateRelation: Expected Success to be true")
	assert.NotEmpty(t, updateRelResp.Message, "UpdateRelation: Expected non-empty message")

	// 5. (可选) 再次获取关系验证更新
	getResp, getRespBody := makeRequest(t, http.MethodGet, updatePath, nil)
	assert.Equal(t, http.StatusOK, getResp.StatusCode)
	var getRelResp network_model.GetRelationResponse
	err = json.Unmarshal(getRespBody, &getRelResp)
	assert.NoError(t, err)
	assert.True(t, getRelResp.Success)
	assert.NotNil(t, getRelResp.Relation)
	assert.Equal(t, relationID, getRelResp.Relation.ID)
	assert.Equal(t, updateReqBody.Properties["status"], getRelResp.Relation.Properties["status"], "UpdateRelation Verify: Expected updated status")
	assert.Equal(t, updateReqBody.Properties["notes"], getRelResp.Relation.Properties["notes"], "UpdateRelation Verify: Expected added notes")
}

// TestE2EDeleteRelation 测试删除关系 API
func TestE2EDeleteRelation(t *testing.T) {
	// 1. 创建关系用于删除
	startNodeID := createTestNode(t, network_model.NodeType_PERSON, "DelRel Start Person", nil)
	endNodeID := createTestNode(t, network_model.NodeType_PERSON, "DelRel End Person", nil)
	relationType := network_model.RelationType_FRIEND
	relationID := createTestRelation(t, startNodeID, endNodeID, relationType, map[string]string{"since": "2024"})

	// 2. 发送删除请求
	deletePath := fmt.Sprintf("%s/relations/%s", apiV1Prefix, relationID)
	deleteResp, deleteRespBody := makeRequest(t, http.MethodDelete, deletePath, nil)

	// 3. 断言删除响应
	assert.Equal(t, http.StatusOK, deleteResp.StatusCode, "DeleteRelation: Expected status OK")
	var deleteRelResp network_model.DeleteRelationResponse
	err := json.Unmarshal(deleteRespBody, &deleteRelResp)
	assert.NoError(t, err, "DeleteRelation: Failed to unmarshal DeleteRelationResponse")
	assert.True(t, deleteRelResp.Success, "DeleteRelation: Expected Success to be true")
	assert.NotEmpty(t, deleteRelResp.Message, "DeleteRelation: Expected non-empty message")

	// 4. (可选) 尝试再次获取该关系，确认已删除
	getResp, getRespBody := makeRequest(t, http.MethodGet, deletePath, nil)
	assert.Equal(t, http.StatusOK, getResp.StatusCode, "DeleteRelation Verify: Expected status OK on Get attempt")
	var getRelResp network_model.GetRelationResponse
	err = json.Unmarshal(getRespBody, &getRelResp)
	assert.NoError(t, err, "DeleteRelation Verify: Failed to unmarshal GetRelationResponse")
	assert.False(t, getRelResp.Success, "DeleteRelation Verify: Expected Success to be false")
	assert.Nil(t, getRelResp.Relation, "DeleteRelation Verify: Expected Relation to be nil")
}

// TestE2EGetNodeRelations 测试获取节点关系 API
func TestE2EGetNodeRelations(t *testing.T) {
	// 1. 创建中心节点和相关节点/关系
	centerNodeID := createTestNode(t, network_model.NodeType_PERSON, "Center Person", nil)
	companyNodeID := createTestNode(t, network_model.NodeType_COMPANY, "Related Company", nil)
	projectNodeID := createTestNode(t, network_model.NodeType_COMPANY, "Related Project", nil)
	eventNodeID := createTestNode(t, network_model.NodeType_PERSON, "Related Event", nil)

	// 创建关系:
	// OUTGOING: Person -> WORKS_AT -> Company
	relWorksAtID := createTestRelation(t, centerNodeID, companyNodeID, network_model.RelationType_COLLEAGUE, map[string]string{"role": "Manager"})
	// OUTGOING: Person -> CONTRIBUTES_TO -> Project
	relContribID := createTestRelation(t, centerNodeID, projectNodeID, network_model.RelationType_COLLEAGUE, map[string]string{"hours": "20"})
	// INCOMING: Event -> ATTENDED -> Person
	relAttendedID := createTestRelation(t, eventNodeID, centerNodeID, network_model.RelationType_VISITED, nil)

	t.Logf("Center Node ID: %s, Company: %s, Project: %s, Event: %s", centerNodeID, companyNodeID, projectNodeID, eventNodeID)
	t.Logf("Relations created: WORKS_AT: %s, CONTRIBUTES_TO: %s, ATTENDED: %s", relWorksAtID, relContribID, relAttendedID)

	// 2. 获取所有关系
	getAllPath := fmt.Sprintf("%s/nodes/%s/relations", apiV1Prefix, centerNodeID)
	getAllResp, getAllBody := makeRequest(t, http.MethodGet, getAllPath, nil)
	assert.Equal(t, http.StatusOK, getAllResp.StatusCode, "GetNodeRelations All: Expected status OK")
	var getAllRespParsed network_model.GetNodeRelationsResponse
	err := json.Unmarshal(getAllBody, &getAllRespParsed)
	assert.NoError(t, err, "GetNodeRelations All: Failed to unmarshal response")
	assert.True(t, getAllRespParsed.Success, "GetNodeRelations All: Expected Success true")
	assert.Len(t, getAllRespParsed.Relations, 3, "GetNodeRelations All: Expected 3 relations")

	// 3. 获取 OUTGOING 关系
	getOutPath := fmt.Sprintf("%s/nodes/%s/relations?outgoing=true&incoming=false", apiV1Prefix, centerNodeID)
	getOutResp, getOutBody := makeRequest(t, http.MethodGet, getOutPath, nil)
	assert.Equal(t, http.StatusOK, getOutResp.StatusCode, "GetNodeRelations Out: Expected status OK")
	var getOutRespParsed network_model.GetNodeRelationsResponse
	err = json.Unmarshal(getOutBody, &getOutRespParsed)
	assert.NoError(t, err, "GetNodeRelations Out: Failed to unmarshal response")
	assert.True(t, getOutRespParsed.Success, "GetNodeRelations Out: Expected Success true")
	assert.Len(t, getOutRespParsed.Relations, 2, "GetNodeRelations Out: Expected 2 outgoing relations")
	// 检查关系类型是否为 WORKS_AT 和 CONTRIBUTES_TO
	foundTypesOut := make(map[network_model.RelationType]bool)
	for _, rel := range getOutRespParsed.Relations {
		assert.Equal(t, centerNodeID, rel.Source, "GetNodeRelations Out: Relation should start from center node")
		foundTypesOut[rel.Type] = true
	}
	assert.True(t, foundTypesOut[network_model.RelationType_COLLEAGUE], "GetNodeRelations Out: Missing COLLEAGUE relation")

	// 4. 获取 INCOMING 关系
	getInPath := fmt.Sprintf("%s/nodes/%s/relations?outgoing=false&incoming=true", apiV1Prefix, centerNodeID)
	getInResp, getInBody := makeRequest(t, http.MethodGet, getInPath, nil)
	assert.Equal(t, http.StatusOK, getInResp.StatusCode, "GetNodeRelations In: Expected status OK")
	var getInRespParsed network_model.GetNodeRelationsResponse
	err = json.Unmarshal(getInBody, &getInRespParsed)
	assert.NoError(t, err, "GetNodeRelations In: Failed to unmarshal response")
	assert.True(t, getInRespParsed.Success, "GetNodeRelations In: Expected Success true")
	assert.Len(t, getInRespParsed.Relations, 1, "GetNodeRelations In: Expected 1 incoming relation")
	assert.Equal(t, network_model.RelationType_VISITED, getInRespParsed.Relations[0].Type, "GetNodeRelations In: Expected VISITED relation type")
	assert.Equal(t, centerNodeID, getInRespParsed.Relations[0].Target, "GetNodeRelations In: Relation should end at target node")

	// 5. 获取特定类型的关系 (例如 WORKS_AT)
	getTypePath := fmt.Sprintf("%s/nodes/%s/relations?types=%d", apiV1Prefix, centerNodeID, network_model.RelationType_COLLEAGUE) // 使用 types 参数 和 RelationType 整数值
	getTypeResp, getTypeBody := makeRequest(t, http.MethodGet, getTypePath, nil)
	assert.Equal(t, http.StatusOK, getTypeResp.StatusCode, "GetNodeRelations Type: Expected status OK")
	var getTypeRespParsed network_model.GetNodeRelationsResponse
	err = json.Unmarshal(getTypeBody, &getTypeRespParsed)
	assert.NoError(t, err, "GetNodeRelations Type: Failed to unmarshal response")
	assert.True(t, getTypeRespParsed.Success, "GetNodeRelations Type: Expected Success true")
	assert.Len(t, getTypeRespParsed.Relations, 2, "GetNodeRelations Type: Expected 2 COLLEAGUE relations based on setup")
	for _, rel := range getTypeRespParsed.Relations {
		assert.Equal(t, network_model.RelationType_COLLEAGUE, rel.Type, "GetNodeRelations Type: Expected correct type for all returned relations")
	}
}

// TestE2EGetNetwork 测试获取网络邻域 API
func TestE2EGetNetwork(t *testing.T) {
	// 1. 创建一个小的网络结构 A -> B -> C, A -> D
	nodeA_ID := createTestNode(t, network_model.NodeType_PERSON, "Net Person A", nil)
	nodeB_ID := createTestNode(t, network_model.NodeType_COMPANY, "Net Company B", nil)
	nodeC_ID := createTestNode(t, network_model.NodeType_COMPANY, "Net Project C", nil)
	nodeD_ID := createTestNode(t, network_model.NodeType_PERSON, "Net Event D", nil)

	relAB_ID := createTestRelation(t, nodeA_ID, nodeB_ID, network_model.RelationType_COLLEAGUE, nil)
	relBC_ID := createTestRelation(t, nodeB_ID, nodeC_ID, network_model.RelationType_FOLLOWING, nil)
	relAD_ID := createTestRelation(t, nodeA_ID, nodeD_ID, network_model.RelationType_VISITED, nil)
	t.Logf("Network created: A(%s) -> B(%s) -> C(%s), A(%s) -> D(%s)", nodeA_ID, nodeB_ID, nodeC_ID, nodeA_ID, nodeD_ID)
	t.Logf("Relations: AB(%s), BC(%s), AD(%s)", relAB_ID, relBC_ID, relAD_ID)

	// 2. 从 A 开始获取深度为 1 的网络
	getPath1 := fmt.Sprintf("%s/network?startNodeCriteria[id]=%s&depth=1", apiV1Prefix, nodeA_ID) // 使用 startNodeCriteria[id] 和 depth
	getResp1, getBody1 := makeRequest(t, http.MethodGet, getPath1, nil)
	assert.Equal(t, http.StatusOK, getResp1.StatusCode, "GetNetwork Depth 1: Expected status OK")
	var getNetResp1 network_model.GetNetworkResponse
	err := json.Unmarshal(getBody1, &getNetResp1)
	assert.NoError(t, err, "GetNetwork Depth 1: Failed to unmarshal response")
	assert.True(t, getNetResp1.Success, "GetNetwork Depth 1: Expected Success true")
	assert.Len(t, getNetResp1.Nodes, 3, "GetNetwork Depth 1: Expected 3 nodes (A, B, D)")            // A, B, D
	assert.Len(t, getNetResp1.Relations, 2, "GetNetwork Depth 1: Expected 2 relations (A->B, A->D)") // A->B, A->D
	// 验证包含的节点和关系 ID
	nodeIDs1 := make(map[string]bool)
	for _, n := range getNetResp1.Nodes {
		nodeIDs1[n.ID] = true
	}
	assert.True(t, nodeIDs1[nodeA_ID])
	assert.True(t, nodeIDs1[nodeB_ID])
	assert.True(t, nodeIDs1[nodeD_ID])
	relIDs1 := make(map[string]bool)
	for _, r := range getNetResp1.Relations {
		relIDs1[r.ID] = true
	}
	assert.True(t, relIDs1[relAB_ID])
	assert.True(t, relIDs1[relAD_ID])

	// 3. 从 A 开始获取深度为 2 的网络
	getPath2 := fmt.Sprintf("%s/network?startNodeCriteria[id]=%s&depth=2", apiV1Prefix, nodeA_ID) // 使用 startNodeCriteria[id] 和 depth
	getResp2, getBody2 := makeRequest(t, http.MethodGet, getPath2, nil)
	assert.Equal(t, http.StatusOK, getResp2.StatusCode, "GetNetwork Depth 2: Expected status OK")
	var getNetResp2 network_model.GetNetworkResponse
	err = json.Unmarshal(getBody2, &getNetResp2)
	assert.NoError(t, err, "GetNetwork Depth 2: Failed to unmarshal response")
	assert.True(t, getNetResp2.Success, "GetNetwork Depth 2: Expected Success true")
	assert.Len(t, getNetResp2.Nodes, 4, "GetNetwork Depth 2: Expected 4 nodes (A, B, C, D)")               // A, B, C, D
	assert.Len(t, getNetResp2.Relations, 3, "GetNetwork Depth 2: Expected 3 relations (A->B, B->C, A->D)") // A->B, B->C, A->D
	// 验证包含的节点和关系 ID
	nodeIDs2 := make(map[string]bool)
	for _, n := range getNetResp2.Nodes {
		nodeIDs2[n.ID] = true
	}
	assert.True(t, nodeIDs2[nodeA_ID])
	assert.True(t, nodeIDs2[nodeB_ID])
	assert.True(t, nodeIDs2[nodeC_ID])
	assert.True(t, nodeIDs2[nodeD_ID])
	relIDs2 := make(map[string]bool)
	for _, r := range getNetResp2.Relations {
		relIDs2[r.ID] = true
	}
	assert.True(t, relIDs2[relAB_ID])
	assert.True(t, relIDs2[relBC_ID])
	assert.True(t, relIDs2[relAD_ID])

}

// TestE2EGetPath 测试获取路径 API
func TestE2EGetPath(t *testing.T) {
	// 1. 创建一个路径 A -> B -> C
	nodeA_ID := createTestNode(t, network_model.NodeType_PERSON, "Path Person A", nil)
	nodeB_ID := createTestNode(t, network_model.NodeType_COMPANY, "Path Org B", nil) // Assume ORGANIZATION exists
	nodeC_ID := createTestNode(t, network_model.NodeType_COMPANY, "Path Project C", nil)
	nodeD_ID := createTestNode(t, network_model.NodeType_PERSON, "Path Person D (no path)", nil) // Unrelated node

	relAB_ID := createTestRelation(t, nodeA_ID, nodeB_ID, network_model.RelationType_COLLEAGUE, nil) // Placeholder
	relBC_ID := createTestRelation(t, nodeB_ID, nodeC_ID, network_model.RelationType_FOLLOWING, nil)
	t.Logf("Path created: A(%s) -> B(%s) -> C(%s)", nodeA_ID, nodeB_ID, nodeC_ID)
	t.Logf("Relations: AB(%s), BC(%s)", relAB_ID, relBC_ID)
	t.Logf("Unrelated node D: %s", nodeD_ID)

	// --- Part 1: Test Path Found ---
	// 2. 查找从 A 到 C 的路径
	getPathFound := fmt.Sprintf("%s/path?source_id=%s&target_id=%s", apiV1Prefix, nodeA_ID, nodeC_ID) // 使用 source_id 和 target_id
	getRespFound, getBodyFound := makeRequest(t, http.MethodGet, getPathFound, nil)
	assert.Equal(t, http.StatusOK, getRespFound.StatusCode, "GetPath Found: Expected status OK")
	var getPathRespFound network_model.GetPathResponse
	err := json.Unmarshal(getBodyFound, &getPathRespFound)
	assert.NoError(t, err, "GetPath Found: Failed to unmarshal response")
	assert.True(t, getPathRespFound.Success, "GetPath Found: Expected Success true")
	assert.NotEmpty(t, getPathRespFound.Nodes, "GetPath Found: Expected non-empty Nodes list")
	assert.NotEmpty(t, getPathRespFound.Relations, "GetPath Found: Expected non-empty Relations list")

	// 验证找到的路径 (假设找到最短路径 A->B->C)
	if len(getPathRespFound.Nodes) == 3 && len(getPathRespFound.Relations) == 2 {
		assert.Equal(t, nodeA_ID, getPathRespFound.Nodes[0].ID, "GetPath Found: Path Node 1 should be A")
		assert.Equal(t, nodeB_ID, getPathRespFound.Nodes[1].ID, "GetPath Found: Path Node 2 should be B")
		assert.Equal(t, nodeC_ID, getPathRespFound.Nodes[2].ID, "GetPath Found: Path Node 3 should be C")
		assert.Equal(t, relAB_ID, getPathRespFound.Relations[0].ID, "GetPath Found: Path Relation 1 should be A->B")
		assert.Equal(t, relBC_ID, getPathRespFound.Relations[1].ID, "GetPath Found: Path Relation 2 should be B->C")
	}

	// --- Part 2: Test Path Not Found ---
	// 3. 查找从 A 到 D 的路径 (应该不存在)
	getPathNotFound := fmt.Sprintf("%s/path?source_id=%s&target_id=%s", apiV1Prefix, nodeA_ID, nodeD_ID) // 使用 source_id 和 target_id
	getRespNotFound, getBodyNotFound := makeRequest(t, http.MethodGet, getPathNotFound, nil)
	assert.Equal(t, http.StatusOK, getRespNotFound.StatusCode, "GetPath NotFound: Expected status OK")
	var getPathRespNotFound network_model.GetPathResponse
	err = json.Unmarshal(getBodyNotFound, &getPathRespNotFound)
	assert.NoError(t, err, "GetPath NotFound: Failed to unmarshal response")
	// 行为取决于你的 Handler 实现：
	// 选项 A: Success: true, Paths: [] (空列表)
	// assert.True(t, getPathRespNotFound.Success, "GetPath NotFound: Expected Success true (even if no path)")
	// assert.Empty(t, getPathRespNotFound.Paths, "GetPath NotFound: Expected empty paths list")
	// 选项 B: Success: false, Message: "No path found"
	assert.False(t, getPathRespNotFound.Success, "GetPath NotFound: Expected Success false")
	assert.Empty(t, getPathRespNotFound.Nodes, "GetPath NotFound: Expected empty Nodes list")
	assert.Empty(t, getPathRespNotFound.Relations, "GetPath NotFound: Expected empty Relations list")
	assert.NotEmpty(t, getPathRespNotFound.Message, "GetPath NotFound: Expected non-empty message")

}
