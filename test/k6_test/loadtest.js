import http from 'k6/http';
import { check, sleep } from 'k6';
import { SharedArray } from 'k6/data';
import { Trend } from 'k6/metrics'; // Import Trend for custom metrics

// --- 配置 ---
const BASE_URL = 'http://localhost:8888'; // !! 修改为你的服务实际地址和端口 !!
const API_V1_PREFIX = '/api/v1';
const TEST_DEPTH = 1; // 测试 GetNetwork 的深度

// --- 自定义指标 (可选，但推荐) ---
// 为 GetNetwork 和 CreateNode 分别创建响应时间趋势指标
const getNetworkDuration = new Trend('http_req_duration_getnetwork', true);
const createNodeDuration = new Trend('http_req_duration_createnode', true);

// --- 测试阶段 (梯度增加负载) ---
export const options = {
  thresholds: {
    'http_req_failed': ['rate<0.20'], // 整体失败率 < 1%
    'checks': ['rate>0.80'],          // 整体检查成功率 > 98%

    // 针对 GetNetwork 的阈值
    'http_req_duration_getnetwork': ['p(95)<1000'], // p95 < 1000ms
    'checks{name:GetNetworkChecks}': ['rate>0.80'], // GetNetwork 检查成功率 > 98%

    // 针对 CreateNode 的阈值 (写操作通常慢一些)
    'http_req_duration_createnode': ['p(95)<1500'], // p95 < 1500ms
    'checks{name:CreateNodeChecks}': ['rate>0.80'], // CreateNode 检查成功率 > 98%
  },
  scenarios: {
    default: {
      executor: 'ramping-vus',
      stages: [ // 使用你之前调整的更高负载
        { duration: '30s', target: 2000 }, // 保持较低的起始值以便观察
        { duration: '1m', target: 2000 },
        { duration: '10s', target: 5000 },
        { duration: '1m', target: 5000 },
        { duration: '10s', target: 10000 },
        { duration: '1m', target: 10000 },
        { duration: '20s', target: 0 },
       ],
      gracefulRampDown: '30s',
    },
  },
};

// --- Helper Function: 创建节点 ---
// (复用并略作修改以返回完整响应，方便获取 ID)
function createNode(nodeType, namePrefix, props) {
  const uniqueName = `${namePrefix}-${Date.now()}-${Math.random()}`;
  const payload = JSON.stringify({
    type: nodeType,
    name: uniqueName,
    properties: { ...props, source: 'k6-setup-getnetwork' }
  });
  const params = { headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' } };
  return http.post(`${BASE_URL}${API_V1_PREFIX}/nodes`, payload, params);
}

// --- Helper Function: 创建关系 ---
function createRelation(sourceId, targetId, relType, props) {
    const payload = JSON.stringify({
        source: sourceId,
        target: targetId,
        type: relType,
        properties: { ...props, source: 'k6-setup-getnetwork-rel' }
    });
    const params = { headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' } };
    return http.post(`${BASE_URL}${API_V1_PREFIX}/relations`, payload, params);
}

// --- Setup Function: 创建测试网络 A->B, A->C ---
export function setup() {
  console.log(`压测准备 (读写混合): 正在创建测试网络 (A -> B, A -> C)...`);
  // NodeType 和 RelationType 使用整数值，假设与你的定义匹配
  const nodeTypePerson = 1;
  const nodeTypeCompany = 2;
  const relTypeColleague = 1; // 假设 COLLEAGUE = 1
  const relTypeVisited = 4;   // 假设 VISITED = 4

  // 1. 创建节点 A, B, C
  const resA = createNode(nodeTypePerson, 'NetNodeA', { group: 'test' });
  const resB = createNode(nodeTypeCompany, 'NetNodeB', { group: 'test' });
  const resC = createNode(nodeTypePerson, 'NetNodeC', { group: 'test' });

  // 检查节点创建是否成功并获取 ID
  let nodeAId, nodeBId, nodeCId;
  const createdNodeIds = [];
  const createdRelationIds = []; // 用于 teardown

  if (resA.status === 200 && resA.json() && resA.json().success && resA.json().node) {
    nodeAId = resA.json().node.id;
    createdNodeIds.push(nodeAId);
    console.log(`  - 创建节点 A 成功: ${nodeAId}`);
  } else {
    console.error(`  - 创建节点 A 失败! Status: ${resA.status}, Body: ${resA.body}`);
    return { error: 'Failed to create node A in setup' };
  }
  if (resB.status === 200 && resB.json() && resB.json().success && resB.json().node) {
    nodeBId = resB.json().node.id;
    createdNodeIds.push(nodeBId);
    console.log(`  - 创建节点 B 成功: ${nodeBId}`);
  } else {
    console.error(`  - 创建节点 B 失败! Status: ${resB.status}, Body: ${resB.body}`);
    // 尝试清理已创建的节点A
    if (nodeAId) http.del(`${BASE_URL}${API_V1_PREFIX}/nodes/${nodeAId}`);
    return { error: 'Failed to create node B in setup', cleanupIds: createdNodeIds }; // 传递已创建的ID
  }
    if (resC.status === 200 && resC.json() && resC.json().success && resC.json().node) {
    nodeCId = resC.json().node.id;
    createdNodeIds.push(nodeCId);
    console.log(`  - 创建节点 C 成功: ${nodeCId}`);
  } else {
    console.error(`  - 创建节点 C 失败! Status: ${resC.status}, Body: ${resC.body}`);
    // 尝试清理已创建的节点A, B
    createdNodeIds.forEach(id => http.del(`${BASE_URL}${API_V1_PREFIX}/nodes/${id}`));
    return { error: 'Failed to create node C in setup', cleanupIds: createdNodeIds };
  }


  // 2. 创建关系 A->B, A->C
  const resAB = createRelation(nodeAId, nodeBId, relTypeColleague, { role: 'tester' });
  const resAC = createRelation(nodeAId, nodeCId, relTypeVisited, { reason: 'testing' });

  if (resAB.status === 200 && resAB.json() && resAB.json().success && resAB.json().relation) {
     createdRelationIds.push(resAB.json().relation.id);
     console.log(`  - 创建关系 A->B 成功: ${createdRelationIds[createdRelationIds.length - 1]}`);
  } else {
      console.error(`  - 创建关系 A->B 失败! Status: ${resAB.status}, Body: ${resAB.body}`);
      // 清理所有已创建节点和可能已创建的关系
      createdNodeIds.forEach(id => http.del(`${BASE_URL}${API_V1_PREFIX}/nodes/${id}`));
      return { error: 'Failed to create relation A->B in setup', cleanupIds: createdNodeIds };
  }
    if (resAC.status === 200 && resAC.json() && resAC.json().success && resAC.json().relation) {
     createdRelationIds.push(resAC.json().relation.id);
     console.log(`  - 创建关系 A->C 成功: ${createdRelationIds[createdRelationIds.length - 1]}`);
  } else {
      console.error(`  - 创建关系 A->C 失败! Status: ${resAC.status}, Body: ${resAC.body}`);
      // 清理所有已创建节点和关系 A->B
      createdRelationIds.forEach(id => http.del(`${BASE_URL}${API_V1_PREFIX}/relations/${id}`));
      createdNodeIds.forEach(id => http.del(`${BASE_URL}${API_V1_PREFIX}/nodes/${id}`));
      return { error: 'Failed to create relation A->C in setup', cleanupIds: [...createdNodeIds, ...createdRelationIds] };
  }

  console.log(`压测准备 (读写混合) 完成.`);
  // 返回起始节点 ID 和所有需要清理的资源 ID
  return { startNodeId: nodeAId, nodeIdsToClean: createdNodeIds, relationIdsToClean: createdRelationIds };
}


// --- Default Function: 读写混合场景 ---
export default function (data) {
  // 检查 setup 是否成功
  if (!data || !data.startNodeId || data.error) {
    console.error(`VU ${__VU} Iter ${__ITER}: Setup 失败，跳过本轮迭代。错误: ${data.error || 'Unknown setup error'}`);
    check(false, { 'Setup Succeeded': () => false });
    sleep(1);
    return;
  }

  const startNodeId = data.startNodeId;

  // === 1. GetNetwork (读取操作) ===
  const getNetworkUrl = `${BASE_URL}${API_V1_PREFIX}/network?startNodeCriteria[id]=${startNodeId}&depth=${TEST_DEPTH}`;
  const getNetworkParams = {
    headers: { 'Accept': 'application/json' },
    tags: { name: 'GetNetwork', operation: 'read' }, // 添加标签区分操作类型
  };
  const getNetworkRes = http.get(getNetworkUrl, getNetworkParams);

  // 添加到自定义趋势指标
  getNetworkDuration.add(getNetworkRes.timings.duration);

  // GetNetwork 检查 (使用特定 tag)
  const getNetworkCheckRes = check(getNetworkRes, {
    'GetNetwork 状态码是 200': (r) => r.status === 200,
    'GetNetwork 返回 success 为 true': (r) => r.status === 200 && r.json()?.success === true,
    'GetNetwork 返回 nodes 数组': (r) => r.status === 200 && Array.isArray(r.json()?.nodes),
    'GetNetwork 返回 relations 数组': (r) => r.status === 200 && Array.isArray(r.json()?.relations),
    'GetNetwork 返回预期数量的节点 (3)': (r) => r.status === 200 && r.json()?.nodes?.length === 3,
    'GetNetwork 返回预期数量的关系 (2)': (r) => r.status === 200 && r.json()?.relations?.length === 2,
  }, { name: 'GetNetworkChecks' }); // 将检查分组

  if (!getNetworkCheckRes) {
    console.warn(`VU ${__VU} Iter ${__ITER}: GetNetwork 请求检查失败。URL: ${getNetworkUrl}, 状态码: ${getNetworkRes.status}, 响应体: ${getNetworkRes.body?.substring(0, 150)}...`);
  }

  // 在两次操作之间模拟短暂思考
  sleep(Math.random() * 0.5 + 0.1); // 0.1 到 0.6 秒

  // === 2. CreateNode (写入操作) ===
  const nodeTypeToWrite = 1; // 假设创建 PERSON
  const newNodeName = `WriteTestNode-VU${__VU}-Iter${__ITER}-${Date.now()}`;
  const createNodePayload = JSON.stringify({
      type: nodeTypeToWrite,
      name: newNodeName,
      properties: { source: 'k6-write-test', vu: `${__VU}`, iter: `${__ITER}` }
  });
   const createNodeParams = {
       headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
       tags: { name: 'CreateNode', operation: 'write' }, // 添加标签区分操作类型
   };
  const createNodeRes = http.post(`${BASE_URL}${API_V1_PREFIX}/nodes`, createNodePayload, createNodeParams);

  // 添加到自定义趋势指标
  createNodeDuration.add(createNodeRes.timings.duration);

   // CreateNode 检查 (使用特定 tag)
   const createNodeCheckRes = check(createNodeRes, {
       'CreateNode 状态码是 200': (r) => r.status === 200,
       'CreateNode 返回 success 为 true': (r) => r.status === 200 && r.json()?.success === true,
       'CreateNode 返回非空 Node ID': (r) => r.status === 200 && r.json()?.node?.id != null && r.json()?.node?.id !== '',
   }, { name: 'CreateNodeChecks' }); // 将检查分组

  if (!createNodeCheckRes) {
      console.warn(`VU ${__VU} Iter ${__ITER}: CreateNode 请求检查失败。状态码: ${createNodeRes.status}, 响应体: ${createNodeRes.body?.substring(0, 150)}...`);
  }

  // 模拟操作后的思考时间
  sleep(Math.random() * 1.0 + 0.3); // 0.3 到 1.3 秒
}

// --- Teardown Function: 清理测试数据 ---
export function teardown(data) {
  console.log(`压测清理 (读写混合): 正在清理 Setup 资源...`);
  if (data && !data.error) {
      // 清理关系
      if (data.relationIdsToClean && data.relationIdsToClean.length > 0) {
        console.log(`  - 正在删除 ${data.relationIdsToClean.length} 个关系...`);
        data.relationIdsToClean.forEach(relId => {
            const res = http.del(`${BASE_URL}${API_V1_PREFIX}/relations/${relId}`);
            if(res.status !== 200) {
                console.warn(`  - 删除关系 ${relId} 失败: Status ${res.status}`);
            }
        });
      } else {
          console.log("  - 没有关系需要删除。");
      }

      // 清理节点
      if (data.nodeIdsToClean && data.nodeIdsToClean.length > 0) {
        console.log(`  - 正在删除 ${data.nodeIdsToClean.length} 个节点...`);
        data.nodeIdsToClean.forEach(nodeId => {
          const res = http.del(`${BASE_URL}${API_V1_PREFIX}/nodes/${nodeId}`);
            if(res.status !== 200) {
                console.warn(`  - 删除节点 ${nodeId} 失败: Status ${res.status}`);
            }
        });
      } else {
           console.log("  - 没有节点需要删除。");
      }
  } else if (data && data.error && data.cleanupIds) {
      // 如果 Setup 中途失败，尝试清理已创建的部分资源
      console.warn(`压测清理 (读写混合): Setup 失败，尝试清理部分资源...`);
      data.cleanupIds.forEach(id => {
          // 尝试同时作为节点和关系 ID 删除（因为不知道失败时创建了什么）
           http.del(`${BASE_URL}${API_V1_PREFIX}/nodes/${id}`);
           http.del(`${BASE_URL}${API_V1_PREFIX}/relations/${id}`);
      });
  } else {
      console.log(`压测清理 (读写混合): 无需清理 (Setup 可能失败或未返回有效数据)。`);
  }
  console.log(`压测清理 (读写混合) 完成。`);
}
