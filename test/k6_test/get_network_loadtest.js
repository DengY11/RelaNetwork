import http from 'k6/http';
import { check, sleep } from 'k6';
import { Trend } from 'k6/metrics';

// --- 配置 ---
const BASE_URL = 'http://localhost:8888'; // !! 修改为你的服务实际地址和端口 !!
const API_V1_PREFIX = '/api/v1';
const NUM_NODES_TO_CREATE_IN_SETUP = 50; // 在 setup 阶段创建的节点数量
const NODE_TYPE_PERSON = 1; // 假设 PERSON 节点的整数类型是 1
const NODE_TYPE_COMPANY = 2; // 假设 COMPANY 节点的整数类型是 2

// 从 Thrift 定义中获取的有效关系类型及其整数值
const VALID_RELATION_TYPES = {
  COLLEAGUE: 1,
  SCHOOLMATE: 2,
  FRIEND: 3,
  VISITED: 4,
  FOLLOWING: 5,
};
// 将有效的关系类型整数值放入数组，方便随机选择
const POSSIBLE_RELATION_TYPE_VALUES = Object.values(VALID_RELATION_TYPES);

// --- 自定义指标 ---
const getNetworkDuration = new Trend('http_req_duration_getnetwork_fixed', true);

// --- 测试阶段配置 ---
export const options = {
  thresholds: {
    'http_req_failed{name:GetFixedNetwork}': ['rate<0.01'],     // 对于固定查询，失败率应该更低
    'checks{name:GetFixedNetworkChecks}': ['rate>0.99'],         // 检查成功率应该更高
    'http_req_duration_getnetwork_fixed': ['p(95)<500', 'p(99)<1000'], // 期望固定查询更快 (考虑缓存)
    'checks{name:SetupCreateNodeChecks}': ['rate>0.98'],
    'checks{name:SetupCreateRelationChecks}': ['rate>0.98'],
  },
  scenarios: {
    get_fixed_network_scenario: {
      executor: 'ramping-vus',
      startVUs: 1,
      stages: [
        { duration: '30s', target: 500 },  // 少量VU预热和检查
        { duration: '3m', target: 500 },   // 保持负载
        { duration: '30s', target: 0 },    // Ramp down
      ],
      gracefulRampDown: '30s',
    },
  },
};

// --- Helper Functions ---
function randomItem(arrayOfItems) {
  if (arrayOfItems.length === 0) return undefined; // 处理空数组情况
  return arrayOfItems[Math.floor(Math.random() * arrayOfItems.length)];
}

function randomInt(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}

// --- Setup 阶段: 创建一个固定的小型关系网 ---
export function setup() {
  console.log('Setup: Creating a fixed small relational network...');
  const nodeData = [];
  const fixedNodeDefs = [
    { namePrefix: 'FixedNode-A', type: NODE_TYPE_PERSON },
    { namePrefix: 'FixedNode-B', type: NODE_TYPE_COMPANY },
    { namePrefix: 'FixedNode-C', type: NODE_TYPE_PERSON },
    { namePrefix: 'FixedNode-D', type: NODE_TYPE_COMPANY },
    { namePrefix: 'FixedNode-E', type: NODE_TYPE_PERSON },
  ];

  // 1. 创建节点
  console.log('--- Creating Nodes ---'); // Add section log
  for (let i = 0; i < fixedNodeDefs.length; i++) {
    const def = fixedNodeDefs[i];
    const nodeName = `${def.namePrefix}-${Date.now()}`;
    const payload = JSON.stringify({
      name: nodeName,
      type: def.type,
      properties: { created_by: 'k6-fixed-setup', fixed_label: def.namePrefix },
    });
    const res = http.post(`${BASE_URL}${API_V1_PREFIX}/nodes`, payload, {
      headers: { 'Content-Type': 'application/json' },
      tags: { name: 'CreateNodeForSetup' },
    });
    
    // Add detailed logging for CreateNode response
    let parsedJson = null;
    let nodeIdFromResponse = undefined;
    try {
      parsedJson = res.json(); // Try parsing JSON
      if (parsedJson) {
          // --> Check common ways ID might be nested <--
          nodeIdFromResponse = parsedJson.id || parsedJson.node?.id || parsedJson.Node?.id; 
      }
      console.log(`Setup: CreateNode attempt ${i+1} for ${nodeName}. Status: ${res.status}. Body: ${res.body}. Parsed ID: ${nodeIdFromResponse}`);
    } catch (e) {
      console.error(`Setup: CreateNode attempt ${i+1} for ${nodeName}. Status: ${res.status}. Failed to parse JSON response: ${e}. Body: ${res.body}`);
    }
    // End added logging

    const nodeCheckResult = check(res, { 
        'Setup: CreateNode status 200': (r) => r.status === 200, 
        'Setup: CreateNode success true': (r) => parsedJson?.success === true, 
        // Add a check to ensure an ID was found
        'Setup: CreateNode response contains ID': (r) => !!nodeIdFromResponse 
    }, { name: 'SetupCreateNodeChecks' });
    
    if (nodeCheckResult && nodeIdFromResponse) { // Check the result and that we got an ID
      nodeData.push({ id: nodeIdFromResponse, name: nodeName, type: def.type });
    } else {
        console.error(`Setup: Node ${nodeName} creation check failed or ID missing, not adding to nodeData.`);
    }
    sleep(0.05);
  }
  console.log(`Setup: Finished node creation attempts. Nodes successfully created and stored: ${nodeData.length}`);
  if (nodeData.length !== fixedNodeDefs.length) {
    console.error('ERROR: Not all fixed nodes were created in setup. Aborting further setup.');
    return { fixedNetworkRootId: null, expectedNodesInNetwork: 0, expectedRelationsInNetwork: 0 }; 
  }

  // 2. 创建固定的关系 (nodeData[0] is FixedNode-A, nodeData[1] is FixedNode-B etc.)
  // A -> B (COLLEAGUE)
  // A -> C (FRIEND)
  // B -> D (COLLEAGUE)
  // C -> E (FRIEND)
  // D -> E (COLLEAGUE)
  const relationsToCreate = [
    { sourceIdx: 0, targetIdx: 1, type: VALID_RELATION_TYPES.COLLEAGUE, label: 'A-colleague-B' },
    { sourceIdx: 0, targetIdx: 2, type: VALID_RELATION_TYPES.FRIEND,    label: 'A-friend-C' },
    { sourceIdx: 1, targetIdx: 3, type: VALID_RELATION_TYPES.COLLEAGUE, label: 'B-colleague-D' },
    { sourceIdx: 2, targetIdx: 4, type: VALID_RELATION_TYPES.FRIEND,    label: 'C-friend-E' },
    { sourceIdx: 3, targetIdx: 4, type: VALID_RELATION_TYPES.COLLEAGUE, label: 'D-colleague-E' }, 
  ];
  let createdRelationsCount = 0;
  console.log(`Setup: Attempting to create ${relationsToCreate.length} fixed relations...`);

  for (const relDef of relationsToCreate) {
    if (nodeData[relDef.sourceIdx] && nodeData[relDef.targetIdx]) {
      const sourceId = nodeData[relDef.sourceIdx].id;
      const targetId = nodeData[relDef.targetIdx].id;

      console.log(`Setup: Preparing to create relation '${relDef.label}'. Source ID: '${sourceId}' (Type: ${typeof sourceId}), Target ID: '${targetId}' (Type: ${typeof targetId}), RelType: ${relDef.type}`);

      if (!sourceId || typeof sourceId !== 'string' || sourceId.trim() === '' || 
          !targetId || typeof targetId !== 'string' || targetId.trim() === '') {
        console.error(`Setup: Skipping relation '${relDef.label}' due to invalid source/target ID.`);
        continue;
      }

      const createRelationPayload = JSON.stringify({
        source: sourceId,
        target: targetId,
        type: relDef.type,
        label: relDef.label,
        properties: { created_by: 'k6-fixed-setup-relation' },
      });
      const relRes = http.post(`${BASE_URL}${API_V1_PREFIX}/relations`, createRelationPayload, {
        headers: { 'Content-Type': 'application/json' },
        tags: { name: 'CreateRelationForSetup' },
      });
      const relCheckResult = check(relRes, {
        'Setup: CreateRelation status 200': (r) => r.status === 200,
        'Setup: CreateRelation success true': (r) => r.json()?.success === true,
      }, { name: 'SetupCreateRelationChecks' });
      if (relCheckResult) {
        createdRelationsCount++;
      } else {
        console.error(`Setup: Failed to create fixed relation ${relDef.label}. Status: ${relRes.status}, Body: ${relRes.body}`);
      }
      sleep(0.02);
    }
  }
  console.log(`Setup: Created ${createdRelationsCount} fixed relations out of ${relationsToCreate.length} attempts.`);
  if (createdRelationsCount !== relationsToCreate.length) {
     console.warn('WARN: Not all fixed relations were created successfully in setup.');
  }

  // Network: A(0) --COLLEAGUE--> B(1) --COLLEAGUE--> D(3) --COLLEAGUE--> E(4)
  //          |                                        ^
  //           --FRIEND-----> C(2) --FRIEND--------> E(4)
  // Query from A (nodeData[0].id) with depth 2:
  // Nodes: A, B, C, D, E (5 nodes)
  // Relations: A-B, A-C, B-D, C-E (D-E is depth 3 from A via B-D, or depth 2 via A-C-E)
  // If GetNetwork returns distinct nodes/relations up to depth: A-B, B-D, A-C, C-E. (4 relations)
  // If D-E is also included via A-C-E path at depth 2, then 5 relations.
  // Let's assume distinct elements up to depth from startNode.
  // Path A->B->D: Nodes A,B,D; Relations A-B, B-D
  // Path A->C->E: Nodes A,C,E; Relations A-C, C-E
  // Combined unique nodes: A,B,C,D,E (5 nodes)
  // Combined unique relations: A-B, B-D, A-C, C-E (4 relations)
  // If D-E is also reachable at depth 2 or less from A by any path and included, then we need to confirm.
  // A -> C (1) -> E (2). D is not on this path. D is A -> B (1) -> D (2).
  // Relation D-E is between D (depth 2 from A) and E (depth 2 from A). This means D-E itself *might* be considered part of depth 2 network.
  // This depends on GetNetwork's exact behavior: does it return all relations *between* nodes found up to depth X,
  // or only relations on paths up to depth X?
  // Assuming it returns all relations between nodes found within depth 2 from node A:
  // Nodes within depth 2 from A: A, B (d1), C (d1), D (d2 via B), E (d2 via C).
  // Relations between these nodes: A-B, A-C, B-D, C-E, D-E. (5 relations)

  return {
    fixedNetworkRootId: nodeData.length > 0 ? nodeData[0].id : null, 
    expectedNodesInNetwork: 5, 
    expectedRelationsInNetwork: 5 
  };
}

// --- VU 默认执行函数 ---
export default function (data) {
  if (!data || !data.fixedNetworkRootId) {
    console.log('VU: Fixed network root ID not available from setup, skipping GetNetwork test.');
    sleep(1);
    return;
  }

  const startNodeId = data.fixedNetworkRootId;
  const depth = 2; 

  let queryString = `startNodeCriteria_id=${encodeURIComponent(startNodeId)}&depth=${encodeURIComponent(depth)}`;

  const requestUrl = `${BASE_URL}${API_V1_PREFIX}/network?${queryString}`;
  
  const res = http.get(requestUrl, {
    headers: { 'Accept': 'application/json' },
    tags: { name: 'GetFixedNetwork' }, 
  });

  getNetworkDuration.add(res.timings.duration);

  let responseJson = null;
  try {
    responseJson = res.json();
  } catch (e) {
    console.error(`VU ${__VU} Iter ${__ITER}: Failed to parse JSON response. Status: ${res.status}, Body: ${res.body}`);
  }

  const checks = {
    'GetFixedNetwork 状态码是 200': (r) => r.status === 200,
    'GetFixedNetwork 返回 success 为 true': (r) => r.status === 200 && responseJson?.success === true,
  };
  if (responseJson && responseJson.success) {
    checks['GetFixedNetwork nodes count matches expected'] = (r) => Array.isArray(responseJson.nodes) && responseJson.nodes.length === data.expectedNodesInNetwork;
    checks['GetFixedNetwork relations count matches expected'] = (r) => Array.isArray(responseJson.relations) && responseJson.relations.length === data.expectedRelationsInNetwork;
  }

  check(res, checks, { name: 'GetFixedNetworkChecks' });

  if (res.status !== 200 || !responseJson?.success) {
    // console.log(`VU ${__VU} Iter ${__ITER}: GetFixedNetwork failed. Status: ${res.status}. Body: ${res.body}`);
  }

  sleep(randomInt(1, 2));
}

// --- Teardown 阶段 (可选) ---
export function teardown(data) {
  console.log('Teardown: (Not implemented) Consider cleaning up fixed test data.');
} 
