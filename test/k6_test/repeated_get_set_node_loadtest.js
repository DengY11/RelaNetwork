import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend } from 'k6/metrics';

// --- 配置 ---
const BASE_URL = 'http://localhost:8888'; // !! 修改为你的服务实际地址和端口 !!
const API_V1_PREFIX = '/api/v1';
const NODE_TYPE_PERSON = 1; // 假设 PERSON 节点的整数类型是 1
const GETS_PER_ITERATION = 10; // 每次迭代执行 GET 的次数 (替代 GETS_PER_UPDATE)

// --- 自定义指标 ---
const getNodeDuration = new Trend('http_req_duration_getnode_readonly', true); // 重命名指标
// const updateNodeDuration = new Trend('http_req_duration_updatenode_repeated', true); // 移除 Update 指标

// --- 测试阶段配置 ---
export const options = {
  thresholds: {
    'http_req_failed{scenario:readonly_get_node_ops}': ['rate<0.01'], // 整体失败率 < 1%
    'checks{scenario:readonly_get_node_ops}': ['rate>0.99'],          // 整体检查成功率 > 99%

    'http_req_duration_getnode_readonly': ['p(95)<30', 'p(99)<50'],  // 只读 GET 应该极快 (缓存!)
    'checks{name:GetNodeReadOnlyChecks}': ['rate>0.99'],

    // 移除 Update 相关阈值
    // 'http_req_duration_updatenode_repeated': ['p(95)<1000', 'p(99)<1500'],
    // 'checks{name:UpdateNodeRepeatedChecks}': ['rate>0.99'],
  },
  scenarios: {
    readonly_get_node_ops: { // 重命名场景
      executor: 'ramping-vus',
      stages: [
        { duration: '20s', target: 5000 }, 
        { duration: '30s', target: 5000 },
        { duration: '10s', target: 10000 }, 
        { duration: '30s', target: 10000 },
        { duration: '20s', target: 0 },
      ],
      gracefulRampDown: '20s',
    },
  },
};

// --- Helper Function: 创建节点 (用于 Setup) ---
function createNodeForSetup(namePrefix) {
  const uniqueName = `${namePrefix}-ReadOnlyTestNode-${Date.now()}`;
  const payload = JSON.stringify({
    type: NODE_TYPE_PERSON,
    name: uniqueName,
    properties: { source: 'k6-readonly-get-test' }
  });
  const params = { headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' } };
  const res = http.post(`${BASE_URL}${API_V1_PREFIX}/nodes`, payload, params);
  check(res, { 'Setup: CreateNode status 200': (r) => r.status === 200 });
  if (res.status === 200 && res.json()?.success && res.json()?.node?.id) {
    return res.json().node.id;
  }
  console.error(`Setup: Failed to create node. Status: ${res.status}, Body: ${res.body}`);
  return null;
}

// --- Setup Function: 创建一个测试节点 ---
export function setup() {
  console.log(`压测准备 (ReadOnly Get): 正在创建一个测试节点...`);
  const nodeId = createNodeForSetup('SingleTargetNode');
  if (!nodeId) {
    throw new Error("Setup failed: Could not create the target node for the test.");
  }
  console.log(`压测准备 (ReadOnly Get): 测试节点创建完成, ID: ${nodeId}`);
  return { targetNodeId: nodeId }; 
}

// --- Default Function: 只执行 GET 操作 ---
export default function (data) { 
  if (!data || !data.targetNodeId) {
    console.error(`VU ${__VU} Iter ${__ITER}: Setup 未能提供 targetNodeId，跳过本轮迭代。`);
    check(false, { 'SetupProvidedTargetNodeId': () => false });
    sleep(1);
    return;
  }

  const nodeId = data.targetNodeId;

  // 执行多次 GET
  for (let i = 0; i < GETS_PER_ITERATION; i++) {
    const getNodeRes = http.get(`${BASE_URL}${API_V1_PREFIX}/nodes/${nodeId}`, {
      tags: { name: 'GetNodeReadOnly', operation: 'read' } // 更新 tag
    });
    getNodeDuration.add(getNodeRes.timings.duration);
    check(getNodeRes, {
      'GetNodeReadOnly 状态码是 200': (r) => r.status === 200,
      'GetNodeReadOnly 返回 success 为 true': (r) => r.status === 200 && r.json()?.success === true,
      'GetNodeReadOnly ID 匹配': (r) => r.status === 200 && r.json()?.node?.id === nodeId,
    }, { name: 'GetNodeReadOnlyChecks' }); // 更新 check 名称
    sleep(0.05 + Math.random() * 0.05); // 两次 GET 之间短暂随机 sleep (0.05-0.1s)
  }

  // --- 移除 UPDATE 操作 ---
  /*
  const newName = `UpdatedRepeatedNode-VU${__VU}-Iter${__ITER}-${Date.now()}`;
  const updatePayload = JSON.stringify({
    name: newName,
    properties: { 
      updated_by: `k6-vu${__VU}-repeated`, 
      iteration: String(__ITER) // 将 __ITER 转换为字符串
    }
  });
  const updateNodeRes = http.put(`${BASE_URL}${API_V1_PREFIX}/nodes/${nodeId}`, updatePayload, {
    headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
    tags: { name: 'UpdateNodeRepeated', operation: 'write' }
  });
  updateNodeDuration.add(updateNodeRes.timings.duration);
  check(updateNodeRes, {
    'UpdateNodeRepeated 状态码是 200': (r) => r.status === 200,
    'UpdateNodeRepeated 返回 success 为 true': (r) => r.status === 200 && r.json()?.success === true,
    'UpdateNodeRepeated 名称已更新': (r) => r.status === 200 && r.json()?.node?.name === newName,
  }, { name: 'UpdateNodeRepeatedChecks' });
  */

  // 每次迭代后的 Sleep (可以调整或移除，取决于你想要的请求频率)
  // sleep(Math.random() * 0.1 + 0.05); // 0.05s to 0.15s sleep
}

// --- Teardown Function: 清理测试数据 --- (保持不变)
export function teardown(data) { 
  if (data && data.targetNodeId) {
    console.log(`压测清理 (ReadOnly Get): 正在删除测试节点 ID: ${data.targetNodeId}...`);
    const res = http.del(`${BASE_URL}${API_V1_PREFIX}/nodes/${data.targetNodeId}`);
    if (res.status !== 200) {
      console.warn(`  - Teardown: 删除节点 ${data.targetNodeId} 失败: Status ${res.status}`);
    }
    console.log(`压测清理 (ReadOnly Get): 测试节点 ${data.targetNodeId} 清理请求已发送。`);
  } else {
    console.log(`压测清理 (ReadOnly Get): 无需清理 (没有 targetNodeId 被传递到 teardown)。`);
  }
}
