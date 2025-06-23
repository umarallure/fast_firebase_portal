document.addEventListener('DOMContentLoaded', () => {
    const todayExportBtn = document.getElementById('today-export-btn');
    const exportWorkflow = document.getElementById('export-workflow');
    const subaccountList = document.getElementById('subaccount-list');
    const exportStatus = document.getElementById('export-status');
    const exportBtn = document.getElementById('export-btn');

    let selectedSubaccounts = [];

    todayExportBtn.addEventListener('click', async () => {
        exportWorkflow.style.display = '';
        // Step 1: Fetch and show subaccounts
        let subaccounts = [];
        try {
            subaccounts = await fetch('/api/subaccounts').then(r => r.json());
        } catch (e) {
            exportStatus.innerHTML = 'Failed to load subaccounts.';
            exportBtn.disabled = true;
            return;
        }
        subaccountList.innerHTML = subaccounts.map(acc => `
            <div class="form-check">
                <input class="form-check-input subaccount-checkbox" type="checkbox" value="${acc.id}" id="acc-${acc.id}">
                <label class="form-check-label" for="acc-${acc.id}">${acc.name}</label>
            </div>
        `).join('');
        exportStatus.innerHTML = '';
        exportBtn.disabled = true;
    });

    // Only enable/disable export button on subaccount selection
    subaccountList.addEventListener('change', () => {
        selectedSubaccounts = Array.from(document.querySelectorAll('.subaccount-checkbox:checked')).map(cb => cb.value);
        exportBtn.disabled = selectedSubaccounts.length === 0;
    });

    // Export button logic: fetch all pipelines for selected subaccounts and trigger export
    exportBtn.addEventListener('click', async () => {
        if (selectedSubaccounts.length === 0) return;
        exportStatus.innerHTML = 'Exporting...';

        // Fetch subaccounts to get api_key or other info if needed
        const subaccounts = await fetch('/api/subaccounts').then(r => r.json());
        const selections = [];
        for (const subId of selectedSubaccounts) {
            const sub = subaccounts.find(s => s.id === subId);
            // Fetch all pipelines for this subaccount
            let pipelines = [];
            try {
                const resp = await fetch(`/api/subaccounts/${subId}/pipelines`);
                pipelines = await resp.json();
                if (!Array.isArray(pipelines)) pipelines = [];
            } catch (e) {
                pipelines = [];
            }
            const pipelineIds = pipelines.map(p => p.id);
            if (pipelineIds.length > 0) {
                selections.push({ account_id: subId, api_key: sub.api_key, pipelines: pipelineIds });
            }
        }
        try {
            const response = await fetch('/api/v1/automation/export', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ selections })
            });
            if (!response.ok) throw new Error('Export failed');
            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = 'ghl_export.xlsx';
            document.body.appendChild(a);
            a.click();
            a.remove();
            exportStatus.innerHTML = 'Export complete!';
        } catch (error) {
            exportStatus.innerHTML = 'Export failed.';
        }
    });
});