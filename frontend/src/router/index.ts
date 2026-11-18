import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import TopologyBatchView from '@/views/TopologyBatchView.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/topology-batch', name: 'topology-batch', component: TopologyBatchView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
