// Arr reacquisition — jobs, binding index, and manual replacement.
//
// Polls while any job is still working so the table follows a job from
// blocklist through to the replacement being imported.
class ReacquireManager {
    static ACTIVE = ['queued', 'resolving', 'invalidating', 'blocklisting', 'searching'];
    static WAITING = ['waiting_for_grab', 'waiting_for_download', 'waiting_for_import'];

    static STATUS = {
        queued: {label: 'Queued', badge: 'badge-ghost', icon: 'bi-hourglass'},
        resolving: {label: 'Resolving', badge: 'badge-info', icon: 'bi-search'},
        invalidating: {label: 'Deleting', badge: 'badge-info', icon: 'bi-trash3'},
        blocklisting: {label: 'Blocklisting', badge: 'badge-info', icon: 'bi-slash-circle'},
        searching: {label: 'Searching', badge: 'badge-info', icon: 'bi-binoculars'},
        waiting_for_grab: {label: 'Waiting for grab', badge: 'badge-warning', icon: 'bi-clock'},
        waiting_for_download: {label: 'Downloading', badge: 'badge-warning', icon: 'bi-download'},
        waiting_for_import: {label: 'Waiting for import', badge: 'badge-warning', icon: 'bi-box-arrow-in-down'},
        ready: {label: 'Ready', badge: 'badge-success', icon: 'bi-check-circle'},
        failed: {label: 'Failed', badge: 'badge-error', icon: 'bi-x-circle'},
        cancelled: {label: 'Cancelled', badge: 'badge-ghost', icon: 'bi-dash-circle'},
    };

    constructor() {
        this.api = (window.API || '/api').replace(/\/$/, '');
        this.jobs = [];
        this.selected = null;
        this.timer = null;
        this.searchDebounce = null;
        this.bind();
        this.loadAll();
    }

    bind() {
        const $ = (id) => document.getElementById(id);
        $('refreshJobsBtn')?.addEventListener('click', () => this.loadJobs());
        $('refreshIndexBtn')?.addEventListener('click', () => this.refreshIndex());
        $('reacquireBtn')?.addEventListener('click', () => this.openReacquireModal());
        $('jobStatusFilter')?.addEventListener('change', () => this.renderJobs());
        $('reacquireArr')?.addEventListener('change', () => this.searchBindings());
        $('reacquireQuery')?.addEventListener('input', () => {
            clearTimeout(this.searchDebounce);
            this.searchDebounce = setTimeout(() => this.searchBindings(), 250);
        });
        $('reacquireForm')?.addEventListener('submit', (e) => {
            e.preventDefault();
            this.submitReacquire();
        });
    }

    async loadAll() {
        await Promise.all([this.loadJobs(), this.loadIndex()]);
    }

    // === jobs ===

    async loadJobs() {
        try {
            this.jobs = await this.fetchJSON(`${this.api}/arr/reacquire/jobs`) || [];
        } catch (e) {
            this.jobs = [];
            this.setStatusLine('Reacquisition service is unavailable.');
            this.renderJobs();
            return;
        }
        this.renderCounts();
        this.renderJobs();
        this.scheduleNextPoll();
    }

    scheduleNextPoll() {
        clearTimeout(this.timer);
        const working = this.jobs.some((job) => !this.isTerminal(job.status));
        if (working) {
            this.timer = setTimeout(() => this.loadJobs(), 5000);
        }
    }

    isTerminal(status) {
        return status === 'ready' || status === 'failed' || status === 'cancelled';
    }

    group(status) {
        if (ReacquireManager.ACTIVE.includes(status)) return 'active';
        if (ReacquireManager.WAITING.includes(status)) return 'waiting';
        return status;
    }

    renderCounts() {
        const counts = {active: 0, waiting: 0, ready: 0, failed: 0, cancelled: 0};
        this.jobs.forEach((job) => {
            const group = this.group(job.status);
            if (group in counts) counts[group]++;
        });

        const tiles = [
            {key: 'active', label: 'Working', tone: 'text-info', icon: 'bi-arrow-repeat'},
            {key: 'waiting', label: 'Waiting', tone: 'text-warning', icon: 'bi-clock'},
            {key: 'ready', label: 'Ready', tone: 'text-success', icon: 'bi-check-circle'},
            {key: 'failed', label: 'Failed', tone: 'text-error', icon: 'bi-x-circle'},
            {key: 'cancelled', label: 'Cancelled', tone: 'opacity-60', icon: 'bi-dash-circle'},
        ];
        const grid = document.getElementById('jobCountsGrid');
        if (grid) {
            grid.innerHTML = tiles.map((tile) => `
                <div class="bg-base-200/60 rounded-lg px-3 py-2">
                    <div class="text-[10px] uppercase tracking-wide opacity-60">
                        <i class="bi ${tile.icon} mr-1"></i>${tile.label}
                    </div>
                    <div class="text-xl font-semibold ${tile.tone} mt-1">${counts[tile.key]}</div>
                </div>`).join('');
        }

        const working = counts.active + counts.waiting;
        this.setStatusLine(working > 0
            ? `${working} job${working === 1 ? '' : 's'} in progress.`
            : (this.jobs.length ? 'No job is running.' : 'No reacquisitions yet.'));
    }

    setStatusLine(text) {
        const line = document.getElementById('reacquireStatusLine');
        if (line) line.textContent = text;
    }

    filteredJobs() {
        const filter = document.getElementById('jobStatusFilter')?.value || '';
        if (!filter) return this.jobs;
        return this.jobs.filter((job) => this.group(job.status) === filter);
    }

    renderJobs() {
        const body = document.getElementById('jobsTableBody');
        if (!body) return;
        const jobs = this.filteredJobs();
        const count = document.getElementById('jobsCount');
        if (count) count.textContent = this.jobs.length;

        if (!jobs.length) {
            body.innerHTML = `<tr><td colspan="6" class="text-center py-8 opacity-60">
                No jobs to show. A stream or repair failure queues one automatically.</td></tr>`;
            return;
        }

        body.innerHTML = jobs.map((job) => {
            const binding = (job.bindings || [])[0] || {};
            const file = binding.entryFileName || job.fileId || '-';
            const entry = binding.entryName || '';
            return `
                <tr>
                    <td>${this.statusBadge(job.status)}</td>
                    <td class="max-w-xs">
                        <div class="truncate font-medium" title="${this.escape(file)}">${this.escape(file)}</div>
                        ${entry ? `<div class="truncate text-xs opacity-60" title="${this.escape(entry)}">${this.escape(entry)}</div>` : ''}
                    </td>
                    <td>
                        <div class="text-sm">${this.escape(job.arrName || '-')}</div>
                        ${this.arrKind(job.arrName, job.arrType)
                            ? `<div class="text-xs opacity-60">${this.escape(job.arrType)}</div>` : ''}
                    </td>
                    <td><span class="badge badge-ghost badge-sm">${this.escape(job.cause || '-')}</span></td>
                    <td class="text-xs whitespace-nowrap">${this.formatTime(job.updatedAt)}</td>
                    <td class="text-right">
                        <button class="btn btn-ghost btn-xs" onclick="window.reacquireManager.showJob('${job.id}')">
                            <i class="bi bi-eye"></i>
                        </button>
                    </td>
                </tr>`;
        }).join('');
    }

    statusBadge(status) {
        const meta = ReacquireManager.STATUS[status] || {label: status, badge: 'badge-ghost', icon: 'bi-question'};
        const spin = ReacquireManager.ACTIVE.includes(status) ? ' animate-spin' : '';
        return `<span class="badge ${meta.badge} badge-sm gap-1 whitespace-nowrap">
            <i class="bi ${meta.icon}${spin}"></i>${meta.label}</span>`;
    }

    showJob(id) {
        const job = this.jobs.find((candidate) => candidate.id === id);
        const body = document.getElementById('jobDetailBody');
        const modal = document.getElementById('jobDetailModal');
        if (!job || !body || !modal) return;

        const rows = [
            ['Status', this.statusBadge(job.status)],
            ['Arr', this.arrKind(job.arrName, job.arrType)
                ? `${this.escape(job.arrName)} <span class="opacity-60">(${this.escape(job.arrType)})</span>`
                : this.escape(job.arrName || '-')],
            ['Source', this.escape(job.cause || '-')],
            ['Strategy', this.escape(job.strategy || '-')],
            ['Broken download', job.downloadId ? `<code class="text-xs">${this.escape(job.downloadId)}</code>` : '-'],
            ['Replacement', job.replacementDownloadId ? `<code class="text-xs">${this.escape(job.replacementDownloadId)}</code>` : '-'],
            ['Created', this.formatTime(job.createdAt)],
            ['Updated', this.formatTime(job.updatedAt)],
            ['Completed', job.completedAt ? this.formatTime(job.completedAt) : '-'],
        ];

        const mutations = (job.mutations || []).map((mutation) => `
            <tr>
                <td class="text-xs">${this.escape(mutation.kind)}</td>
                <td>${mutation.state === 'confirmed'
                    ? '<span class="badge badge-success badge-xs">confirmed</span>'
                    : '<span class="badge badge-warning badge-xs">intent</span>'}</td>
                <td class="text-xs">${mutation.attempts || 0}</td>
                <td class="text-xs">${mutation.confirmedAt ? this.formatTime(mutation.confirmedAt) : '-'}</td>
            </tr>`).join('');

        const bindings = (job.bindings || []).map((binding) => `
            <tr>
                <td class="text-xs">${this.escape(binding.entryFileName || binding.entryFileId)}</td>
                <td class="text-xs">${binding.arrFileId || '-'}</td>
                <td class="text-xs">${this.escape(binding.confidence || '-')}</td>
            </tr>`).join('');

        body.innerHTML = `
            <div class="grid grid-cols-1 sm:grid-cols-2 gap-x-6 gap-y-2">
                ${rows.map(([label, value]) => `
                    <div class="flex justify-between gap-3 border-b border-base-200 py-1">
                        <span class="text-xs opacity-60">${label}</span>
                        <span class="text-sm text-right break-all">${value}</span>
                    </div>`).join('')}
            </div>
            ${job.lastError ? `
                <div class="alert alert-error py-2">
                    <i class="bi bi-exclamation-octagon"></i>
                    <span class="text-xs break-all">${this.escape(job.lastError)}</span>
                </div>` : ''}
            ${bindings ? `
                <div>
                    <div class="text-xs uppercase tracking-wide opacity-60 mb-1">Files</div>
                    <table class="table table-xs">
                        <thead><tr><th>File</th><th>Arr file</th><th>Match</th></tr></thead>
                        <tbody>${bindings}</tbody>
                    </table>
                </div>` : ''}
            ${mutations ? `
                <div>
                    <div class="text-xs uppercase tracking-wide opacity-60 mb-1">Arr actions</div>
                    <table class="table table-xs">
                        <thead><tr><th>Action</th><th>State</th><th>Attempts</th><th>Confirmed</th></tr></thead>
                        <tbody>${mutations}</tbody>
                    </table>
                </div>` : ''}`;

        this.openModal(modal);
    }

    // === index ===

    async loadIndex() {
        const container = document.getElementById('indexSummary');
        if (!container) return;
        let summaries = [];
        try {
            summaries = await this.fetchJSON(`${this.api}/arr/index`) || [];
        } catch (e) {
            container.innerHTML = `<div class="text-sm opacity-60">Index is unavailable.</div>`;
            return;
        }

        this.fillArrFilter(summaries);
        if (!summaries.length) {
            container.innerHTML = `<div class="text-sm opacity-60 sm:col-span-2 lg:col-span-3">
                Nothing is indexed yet. Only Sonarr and Radarr instances are indexed; rebuild once they hold imported
                files.</div>`;
            return;
        }

        container.innerHTML = summaries.map((summary) => `
            <div class="bg-base-200/60 rounded-lg px-4 py-3">
                <div class="flex items-center justify-between gap-2">
                    <span class="font-medium truncate">${this.escape(summary.arrName)}</span>
                    ${this.arrKind(summary.arrName, summary.arrType)
                        ? `<span class="badge badge-outline badge-sm">${this.escape(summary.arrType)}</span>` : ''}
                </div>
                <div class="mt-2 flex items-baseline gap-2">
                    <span class="text-2xl font-semibold">${summary.bindings}</span>
                    <span class="text-xs opacity-60">files bound</span>
                </div>
                <div class="text-xs opacity-60 mt-1">
                    ${summary.actionable} can be replaced · updated ${this.formatTime(summary.updatedAt)}
                </div>
            </div>`).join('');
    }

    fillArrFilter(summaries) {
        const select = document.getElementById('reacquireArr');
        if (!select) return;
        const current = select.value;
        select.innerHTML = '<option value="">All</option>' +
            summaries.map((summary) => `<option value="${this.escape(summary.arrName)}">${this.escape(summary.arrName)}</option>`).join('');
        select.value = current;
    }

    async refreshIndex() {
        const button = document.getElementById('refreshIndexBtn');
        if (button) button.disabled = true;
        try {
            const res = await fetch(`${this.api}/arr/index/refresh`, {method: 'POST', credentials: 'same-origin'});
            if (!res.ok) throw new Error(`HTTP ${res.status}`);
            this.toast('Index rebuild queued', 'success');
            // The scan is asynchronous; give it a moment before re-reading.
            setTimeout(() => this.loadIndex(), 3000);
        } catch (e) {
            this.toast('Could not queue the index rebuild', 'error');
        } finally {
            if (button) button.disabled = false;
        }
    }

    // === manual reacquire ===

    openReacquireModal() {
        const modal = document.getElementById('reacquireModal');
        if (!modal) return;
        this.selected = null;
        const query = document.getElementById('reacquireQuery');
        if (query) query.value = '';
        document.getElementById('reacquireError')?.classList.add('hidden');
        const submit = document.getElementById('reacquireSubmit');
        if (submit) submit.disabled = true;
        this.openModal(modal);
        this.searchBindings();
    }

    async searchBindings() {
        const body = document.getElementById('bindingResults');
        if (!body) return;
        const query = document.getElementById('reacquireQuery')?.value || '';
        const arrName = document.getElementById('reacquireArr')?.value || '';

        let bindings = [];
        try {
            const params = new URLSearchParams();
            if (query) params.set('q', query);
            if (arrName) params.set('arr', arrName);
            bindings = await this.fetchJSON(`${this.api}/arr/bindings?${params}`) || [];
        } catch (e) {
            body.innerHTML = `<tr><td colspan="4" class="text-center py-6 opacity-60">Search failed.</td></tr>`;
            return;
        }

        if (!bindings.length) {
            body.innerHTML = `<tr><td colspan="4" class="text-center py-6 opacity-60">No indexed file matches.</td></tr>`;
            return;
        }

        body.innerHTML = bindings.map((binding, i) => {
            const media = binding.movieId
                ? `movie ${binding.movieId}`
                : `series ${binding.seriesId || '-'} · season ${binding.seasonNumber ?? '-'}`;
            return `
                <tr class="hover cursor-pointer" onclick="window.reacquireManager.selectBinding(${i})">
                    <td><input type="radio" name="binding" class="radio radio-xs radio-primary"
                               id="binding-${i}" ${binding.arrFileId ? '' : 'disabled'}></td>
                    <td class="max-w-sm">
                        <div class="truncate text-xs font-medium">${this.escape(binding.entryFileName || binding.entryFileId)}</div>
                        <div class="truncate text-[11px] opacity-60">${this.escape(binding.entryName || '')}</div>
                    </td>
                    <td class="text-xs">${this.escape(binding.arrName)}</td>
                    <td class="text-xs opacity-70">${this.escape(media)}</td>
                </tr>`;
        }).join('');
        this.searchResults = bindings;
    }

    selectBinding(index) {
        const binding = (this.searchResults || [])[index];
        if (!binding) return;
        this.selected = binding;
        document.querySelectorAll('#bindingResults input[type="radio"]').forEach((input, i) => {
            input.checked = i === index;
        });
        const submit = document.getElementById('reacquireSubmit');
        if (submit) submit.disabled = false;
    }

    async submitReacquire() {
        if (!this.selected) return;
        const error = document.getElementById('reacquireError');
        const submit = document.getElementById('reacquireSubmit');
        error?.classList.add('hidden');
        if (submit) submit.disabled = true;

        try {
            const res = await fetch(`${this.api}/arr/reacquire`, {
                method: 'POST',
                credentials: 'same-origin',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    entryId: this.selected.entryId,
                    fileId: this.selected.entryFileId,
                    cause: 'manual',
                    strategy: document.getElementById('reacquireStrategy')?.value || 'history_failed',
                }),
            });
            if (!res.ok) {
                const detail = await res.json().catch(() => ({}));
                throw new Error(detail.error || `HTTP ${res.status}`);
            }
            document.getElementById('reacquireModal')?.close();
            this.toast('Reacquisition queued', 'success');
            this.loadJobs();
        } catch (e) {
            if (error) {
                error.textContent = e.message;
                error.classList.remove('hidden');
            }
        } finally {
            if (submit) submit.disabled = false;
        }
    }

    // === helpers ===

    // arrKind reports whether the application type adds anything the instance
    // name does not already say.
    arrKind(name, type) {
        return !!type && String(name).toLowerCase() !== String(type).toLowerCase();
    }

    openModal(modal) {
        if (typeof modal.showModal === 'function') {
            modal.showModal();
        } else {
            modal.setAttribute('open', '');
        }
    }

    async fetchJSON(url) {
        const res = await fetch(url, {credentials: 'same-origin'});
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        return res.json();
    }

    formatTime(value) {
        if (!value) return '-';
        const date = new Date(value);
        if (Number.isNaN(date.getTime()) || date.getFullYear() < 2000) return '-';
        const seconds = Math.floor((Date.now() - date.getTime()) / 1000);
        if (seconds < 60) return 'just now';
        if (seconds < 3600) return `${Math.floor(seconds / 60)}m ago`;
        if (seconds < 86400) return `${Math.floor(seconds / 3600)}h ago`;
        return date.toLocaleString();
    }

    escape(text) {
        if (text === null || text === undefined) return '';
        const div = document.createElement('div');
        div.textContent = String(text);
        return div.innerHTML;
    }

    toast(message, type = 'info') {
        if (typeof window.createToast === 'function') return window.createToast(message, type);
        console.log(`[${type}]`, message);
    }
}

window.ReacquireManager = ReacquireManager;
