/**
 * TaskFlow Dashboard Frontend Application Logic
 * Interacts with live backend REST API endpoints.
 */

const STATE = {
  currentPage: 1,
  pageSize: 15,
  statusFilter: '',
  taskTypeFilter: '',
  autoRefreshInterval: null,
};

// Default sample payloads for built-in tasks
const SAMPLE_PAYLOADS = {
  simulate_report: JSON.stringify({ rows: 500, report_type: "quarterly_revenue" }, null, 2),
  process_text: JSON.stringify({ text: "TaskFlow distributed task processing engine text sample with high performance" }, null, 2),
  simulated_failure: JSON.stringify({ fail_until_attempt: 2, error_message: "Simulated 503 downstream network outage" }, null, 2),
  sleep_task: JSON.stringify({ seconds: 3.0 }, null, 2),
  send_notification: JSON.stringify({ recipient: "devops@example.com", channel: "email" }, null, 2),
};

const TASK_DESCRIPTIONS = {
  simulate_report: "Simulates computational data aggregation and report generation.",
  process_text: "Performs tokenization, word count, and basic NLP sentiment heuristic.",
  simulated_failure: "Configurable failure handler to verify exponential backoff and dead-letter queueing.",
  sleep_task: "Simulates I/O sleep to test concurrency, leases, and cancellation race conditions.",
  send_notification: "Simulates idempotent notification delivery.",
};

document.addEventListener('DOMContentLoaded', () => {
  initEventListeners();
  loadSystemStats();
  loadJobs();
  startAutoRefresh();
});

function initEventListeners() {
  // Task Type Selector change updates sample payload and description
  const taskSelect = document.getElementById('taskTypeSelect');
  taskSelect.addEventListener('change', (e) => {
    const val = e.target.value;
    document.getElementById('payloadInput').value = SAMPLE_PAYLOADS[val] || '{}';
    document.getElementById('taskTypeDescription').innerText = TASK_DESCRIPTIONS[val] || '';
  });

  // Submission Form
  document.getElementById('submitJobForm').addEventListener('submit', handleJobSubmission);

  // Filters
  document.getElementById('filterStatus').addEventListener('change', (e) => {
    STATE.statusFilter = e.target.value;
    STATE.currentPage = 1;
    loadJobs();
  });

  document.getElementById('filterTaskType').addEventListener('change', (e) => {
    STATE.taskTypeFilter = e.target.value;
    STATE.currentPage = 1;
    loadJobs();
  });

  // Refresh Buttons & Toggle
  document.getElementById('manualRefreshBtn').addEventListener('click', () => {
    loadSystemStats();
    loadJobs();
    showToast('Dashboard refreshed', 'info');
  });

  const autoToggle = document.getElementById('autoRefreshToggle');
  autoToggle.addEventListener('change', (e) => {
    if (e.target.checked) {
      startAutoRefresh();
    } else {
      stopAutoRefresh();
    }
  });

  // Pagination
  document.getElementById('prevPageBtn').addEventListener('click', () => {
    if (STATE.currentPage > 1) {
      STATE.currentPage--;
      loadJobs();
    }
  });

  document.getElementById('nextPageBtn').addEventListener('click', () => {
    STATE.currentPage++;
    loadJobs();
  });

  // Modal Close
  document.getElementById('modalCloseBtn').addEventListener('click', closeModal);
  document.getElementById('jobDetailModal').addEventListener('click', (e) => {
    if (e.target.id === 'jobDetailModal') closeModal();
  });
}

function startAutoRefresh() {
  stopAutoRefresh();
  STATE.autoRefreshInterval = setInterval(() => {
    loadSystemStats();
    loadJobs(true); // silent refresh
  }, 3000);
}

function stopAutoRefresh() {
  if (STATE.autoRefreshInterval) {
    clearInterval(STATE.autoRefreshInterval);
    STATE.autoRefreshInterval = null;
  }
}

/**
 * Fetch and update top summary cards and system health
 */
async function loadSystemStats() {
  try {
    const res = await fetch('/api/stats');
    if (!res.ok) throw new Error('Stats request failed');
    const data = await res.json();

    const byStatus = data.jobs_by_status || {};
    const queueDepth = data.queue_depth || {};

    document.getElementById('statQueued').innerText = (byStatus.QUEUED || 0) + (byStatus.PENDING_DISPATCH || 0);
    document.getElementById('statReadyQueue').innerText = queueDepth.ready || 0;

    document.getElementById('statRunning').innerText = byStatus.RUNNING || 0;
    const activeWorkers = data.active_workers || [];
    document.getElementById('statActiveWorkers').innerText = activeWorkers.length;

    document.getElementById('statScheduled').innerText = byStatus.SCHEDULED || 0;
    document.getElementById('statScheduledQueue').innerText = queueDepth.scheduled || 0;

    document.getElementById('statSucceeded').innerText = byStatus.SUCCEEDED || 0;
    document.getElementById('statSuccessRate').innerText = data.success_rate || 100;

    document.getElementById('statFailed').innerText = byStatus.FAILED || 0;
    document.getElementById('statDeadLetter').innerText = queueDepth.dead_letter || 0;

    // Update connection status badge
    updateHealthBadge(true);
  } catch (err) {
    console.error('Failed to load system stats:', err);
    updateHealthBadge(false);
  }
}

function updateHealthBadge(online) {
  const badge = document.getElementById('systemStatusBadge');
  const dot = badge.querySelector('.status-dot');
  const text = document.getElementById('systemStatusText');

  if (online) {
    dot.className = 'status-dot online';
    text.innerText = 'Connected';
  } else {
    dot.className = 'status-dot degraded';
    text.innerText = 'Degraded';
  }
}

/**
 * Fetch and render jobs table
 */
async function loadJobs(silent = false) {
  const tableBody = document.getElementById('jobsTableBody');
  if (!silent) {
    tableBody.innerHTML = '<tr><td colspan="7" class="text-center py-8 text-muted">Loading jobs...</td></tr>';
  }

  try {
    let url = `/api/jobs?page=${STATE.currentPage}&size=${STATE.pageSize}`;
    if (STATE.statusFilter) url += `&status=${encodeURIComponent(STATE.statusFilter)}`;
    if (STATE.taskTypeFilter) url += `&task_type=${encodeURIComponent(STATE.taskTypeFilter)}`;

    const res = await fetch(url);
    if (!res.ok) throw new Error('Failed to fetch jobs');
    const data = await res.json();

    renderJobsTable(data.items || []);
    updatePagination(data.total || 0, data.page, data.pages);
  } catch (err) {
    console.error('Error fetching jobs:', err);
    if (!silent) {
      tableBody.innerHTML = `<tr><td colspan="7" class="text-center py-8 text-muted" style="color: var(--status-failed);">Failed to load jobs: ${err.message}</td></tr>`;
    }
  }
}

function renderJobsTable(jobs) {
  const tbody = document.getElementById('jobsTableBody');
  if (jobs.length === 0) {
    tbody.innerHTML = '<tr><td colspan="7" class="text-center py-8 text-muted">No jobs match the current filters.</td></tr>';
    return;
  }

  tbody.innerHTML = jobs.map((job) => {
    const shortId = job.id.substring(0, 8);
    const createdDate = new Date(job.created_at).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit', second: '2-digit' });

    let actionButtons = `
      <button class="btn btn-secondary btn-sm" onclick="openJobDetail('${job.id}')">View</button>
    `;

    // Eligible for cancel: QUEUED, SCHEDULED, RETRYING
    if (['QUEUED', 'SCHEDULED', 'RETRYING', 'PENDING_DISPATCH'].includes(job.status)) {
      actionButtons += `
        <button class="btn btn-outline btn-sm" style="color: var(--status-failed);" onclick="cancelJob('${job.id}')">Cancel</button>
      `;
    }

    // Eligible for retry: FAILED
    if (job.status === 'FAILED') {
      actionButtons += `
        <button class="btn btn-primary btn-sm" onclick="retryJob('${job.id}')">Retry</button>
      `;
    }

    return `
      <tr>
        <td class="job-id-cell" onclick="openJobDetail('${job.id}')" title="${job.id}">
          ${shortId}...
        </td>
        <td><strong>${escapeHtml(job.task_type)}</strong></td>
        <td>
          <span class="status-badge status-${job.status}">
            ${job.status}
          </span>
        </td>
        <td>${job.priority > 0 ? `+${job.priority}` : job.priority}</td>
        <td>${job.attempt_count} / ${job.max_attempts}</td>
        <td>${createdDate}</td>
        <td>
          <div class="action-buttons">${actionButtons}</div>
        </td>
      </tr>
    `;
  }).join('');
}

function updatePagination(total, page, pages) {
  const info = document.getElementById('paginationInfo');
  const current = document.getElementById('currentPageDisplay');
  const prevBtn = document.getElementById('prevPageBtn');
  const nextBtn = document.getElementById('nextPageBtn');

  info.innerText = `Showing ${Math.min(total, (page - 1) * STATE.pageSize + 1)}-${Math.min(total, page * STATE.pageSize)} of ${total} jobs`;
  current.innerText = `Page ${page} of ${Math.max(1, pages)}`;

  prevBtn.disabled = page <= 1;
  nextBtn.disabled = page >= pages;
}

/**
 * Handle job submission from form
 */
async function handleJobSubmission(e) {
  e.preventDefault();
  const submitBtn = document.getElementById('submitJobBtn');
  submitBtn.disabled = true;
  submitBtn.innerText = 'Submitting...';

  try {
    const taskType = document.getElementById('taskTypeSelect').value;
    const priority = parseInt(document.getElementById('priorityInput').value, 10) || 0;
    const maxAttempts = parseInt(document.getElementById('maxAttemptsInput').value, 10) || 3;
    const delaySeconds = parseInt(document.getElementById('delaySecondsInput').value, 10) || 0;
    const idempotencyKey = document.getElementById('idempotencyKeyInput').value.trim() || null;
    const payloadRaw = document.getElementById('payloadInput').value.trim();

    let payload = {};
    if (payloadRaw) {
      try {
        payload = JSON.parse(payloadRaw);
      } catch (err) {
        showToast('Payload must be valid JSON: ' + err.message, 'error');
        submitBtn.disabled = false;
        submitBtn.innerText = 'Enqueue Job';
        return;
      }
    }

    let runAt = null;
    if (delaySeconds > 0) {
      const futureDate = new Date(Date.now() + delaySeconds * 1000);
      runAt = futureDate.toISOString();
    }

    const body = {
      task_type: taskType,
      payload: payload,
      priority: priority,
      max_attempts: maxAttempts,
      run_at: runAt,
      idempotency_key: idempotencyKey,
    };

    const res = await fetch('/api/jobs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    });

    if (!res.ok) {
      const errData = await res.json();
      throw new Error(errData.detail || 'Submission rejected');
    }

    const createdJob = await res.json();
    showToast(`Job ${createdJob.id.substring(0, 8)}... created (${createdJob.status})`, 'success');

    // Reset idempotency input to prevent accidental duplicate submission
    document.getElementById('idempotencyKeyInput').value = '';

    // Refresh views immediately
    loadSystemStats();
    loadJobs();
  } catch (err) {
    showToast(err.message, 'error');
  } finally {
    submitBtn.disabled = false;
    submitBtn.innerText = 'Enqueue Job';
  }
}

/**
 * Open job details modal
 */
async function openJobDetail(jobId) {
  const modal = document.getElementById('jobDetailModal');
  const modalBody = document.getElementById('modalBody');
  const modalTitle = document.getElementById('modalJobTitle');

  modalTitle.innerText = `Job ${jobId.substring(0, 12)}...`;
  modalBody.innerHTML = '<div class="text-center py-8 text-muted">Loading job details and attempts...</div>';
  modal.classList.add('open');

  try {
    const [jobRes, attemptsRes] = await Promise.all([
      fetch(`/api/jobs/${jobId}`),
      fetch(`/api/jobs/${jobId}/attempts`),
    ]);

    if (!jobRes.ok) throw new Error('Job not found');
    const job = await jobRes.json();
    const attempts = attemptsRes.ok ? await attemptsRes.json() : [];

    renderJobModalContent(job, attempts);
  } catch (err) {
    modalBody.innerHTML = `<div class="text-center py-8" style="color: var(--status-failed);">${err.message}</div>`;
  }
}

function renderJobModalContent(job, attempts) {
  const modalBody = document.getElementById('modalBody');

  let attemptsHtml = '<p class="text-muted text-sm">No recorded attempts yet.</p>';
  if (attempts && attempts.length > 0) {
    attemptsHtml = attempts.map(att => `
      <div class="attempt-card ${att.status}">
        <div style="display: flex; justify-content: space-between; margin-bottom: 0.25rem;">
          <strong>Attempt #${att.attempt_number} &bull; ${att.status}</strong>
          <span>${att.duration_ms !== null ? `${att.duration_ms} ms` : 'In-flight'}</span>
        </div>
        <div style="font-size: 0.75rem; color: var(--text-secondary); margin-bottom: 0.25rem;">
          Worker: <code>${escapeHtml(att.worker_id)}</code> | Started: ${new Date(att.started_at).toLocaleTimeString()}
        </div>
        ${att.error_message ? `<div class="code-block" style="color: #f87171; border-color: rgba(239, 68, 68, 0.3);">${escapeHtml(att.error_message)}</div>` : ''}
      </div>
    `).join('');
  }

  modalBody.innerHTML = `
    <div class="modal-detail-row">
      <div class="modal-detail-label">Job UUID:</div>
      <div class="modal-detail-value font-mono">${job.id}</div>
    </div>
    <div class="modal-detail-row">
      <div class="modal-detail-label">Task Type:</div>
      <div class="modal-detail-value"><strong>${escapeHtml(job.task_type)}</strong></div>
    </div>
    <div class="modal-detail-row">
      <div class="modal-detail-label">Status:</div>
      <div class="modal-detail-value">
        <span class="status-badge status-${job.status}">${job.status}</span>
      </div>
    </div>
    <div class="modal-detail-row">
      <div class="modal-detail-label">Priority:</div>
      <div class="modal-detail-value">${job.priority}</div>
    </div>
    <div class="modal-detail-row">
      <div class="modal-detail-label">Attempts:</div>
      <div class="modal-detail-value">${job.attempt_count} of ${job.max_attempts} max attempts</div>
    </div>
    <div class="modal-detail-row">
      <div class="modal-detail-label">Created At:</div>
      <div class="modal-detail-value">${new Date(job.created_at).toLocaleString()}</div>
    </div>
    ${job.run_at ? `
      <div class="modal-detail-row">
        <div class="modal-detail-label">Scheduled For:</div>
        <div class="modal-detail-value" style="color: var(--status-scheduled);">${new Date(job.run_at).toLocaleString()}</div>
      </div>
    ` : ''}
    ${job.worker_id ? `
      <div class="modal-detail-row">
        <div class="modal-detail-label">Current Worker:</div>
        <div class="modal-detail-value font-mono">${escapeHtml(job.worker_id)}</div>
      </div>
    ` : ''}
    ${job.idempotency_key ? `
      <div class="modal-detail-row">
        <div class="modal-detail-label">Idempotency Key:</div>
        <div class="modal-detail-value font-mono">${escapeHtml(job.idempotency_key)}</div>
      </div>
    ` : ''}
    
    <div style="margin-top: 1rem;">
      <label>Payload</label>
      <div class="code-block">${escapeHtml(JSON.stringify(job.payload, null, 2))}</div>
    </div>

    ${job.result ? `
      <div style="margin-top: 1rem;">
        <label>Execution Result</label>
        <div class="code-block" style="border-color: rgba(16, 185, 129, 0.4);">${escapeHtml(JSON.stringify(job.result, null, 2))}</div>
      </div>
    ` : ''}

    ${job.last_error ? `
      <div style="margin-top: 1rem;">
        <label>Last Error</label>
        <div class="code-block" style="border-color: rgba(239, 68, 68, 0.4); color: #f87171;">${escapeHtml(job.last_error)}</div>
      </div>
    ` : ''}

    <div class="attempts-section">
      <h4>Execution History (${attempts.length} attempts)</h4>
      ${attemptsHtml}
    </div>
  `;
}

function closeModal() {
  document.getElementById('jobDetailModal').classList.remove('open');
}

/**
 * Cancel job action
 */
async function cancelJob(jobId) {
  if (!confirm(`Are you sure you want to cancel job ${jobId.substring(0, 8)}?`)) return;

  try {
    const res = await fetch(`/api/jobs/${jobId}/cancel`, { method: 'POST' });
    if (!res.ok) {
      const err = await res.json();
      throw new Error(err.detail || 'Cancellation failed');
    }
    showToast(`Job ${jobId.substring(0, 8)} cancelled`, 'success');
    loadSystemStats();
    loadJobs();
  } catch (err) {
    showToast(err.message, 'error');
  }
}

/**
 * Retry job action
 */
async function retryJob(jobId) {
  try {
    const res = await fetch(`/api/jobs/${jobId}/retry`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ reset_attempts: false }),
    });
    if (!res.ok) {
      const err = await res.json();
      throw new Error(err.detail || 'Retry failed');
    }
    showToast(`Job ${jobId.substring(0, 8)} queued for retry`, 'success');
    loadSystemStats();
    loadJobs();
  } catch (err) {
    showToast(err.message, 'error');
  }
}

/**
 * Toast notifications
 */
function showToast(message, type = 'info') {
  const container = document.getElementById('toastContainer');
  const toast = document.createElement('div');
  toast.className = `toast ${type}`;
  toast.innerText = message;

  container.appendChild(toast);
  setTimeout(() => {
    toast.style.opacity = '0';
    toast.style.transition = 'opacity 0.3s ease';
    setTimeout(() => toast.remove(), 300);
  }, 3500);
}

function escapeHtml(str) {
  if (!str) return '';
  return String(str)
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#039;');
}
