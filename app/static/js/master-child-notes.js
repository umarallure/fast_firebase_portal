// Master-Child Notes Transfer JavaScript functionality
document.addEventListener('DOMContentLoaded', function() {
    // Global variables to store file contents and matching results
    let masterFileContent = null;
    let childFileContent = null;
    let matchingResults = null;

    // File upload handlers
    const masterFileInput = document.getElementById('masterFile');
    const childFileInput = document.getElementById('childFile');
    const previewMasterBtn = document.getElementById('preview-master-btn');
    const previewChildBtn = document.getElementById('preview-child-btn');
    const startMatchingBtn = document.getElementById('start-matching-btn');
    const startTransferBtn = document.getElementById('start-transfer-btn');

    // Preview Master File
    previewMasterBtn?.addEventListener('click', async function() {
        if (!masterFileInput.files.length) {
            showAlert('Please select a master CSV file first.', 'warning');
            return;
        }

        try {
            const fileContent = await readFileAsText(masterFileInput.files[0]);
            masterFileContent = fileContent;
            
            const preview = parseCSVPreview(fileContent, 'Master');
            document.getElementById('master-preview').innerHTML = preview;
            
            updateStartMatchingButton();
        } catch (error) {
            showAlert('Error reading master file: ' + error.message, 'danger');
        }
    });

    // Preview Child File
    previewChildBtn?.addEventListener('click', async function() {
        if (!childFileInput.files.length) {
            showAlert('Please select a child CSV file first.', 'warning');
            return;
        }

        try {
            const fileContent = await readFileAsText(childFileInput.files[0]);
            childFileContent = fileContent;
            
            const preview = parseCSVPreview(fileContent, 'Child');
            document.getElementById('child-preview').innerHTML = preview;
            
            updateStartMatchingButton();
        } catch (error) {
            showAlert('Error reading child file: ' + error.message, 'danger');
        }
    });

    // Start Contact Matching
    startMatchingBtn?.addEventListener('click', async function() {
        if (!masterFileContent || !childFileContent) {
            showAlert('Please upload and preview both files first.', 'warning');
            return;
        }

        // Switch to matching tab
        document.getElementById('matching-tab').click();
        
        // Show processing status
        const matchingStatus = document.getElementById('matching-status');
        matchingStatus.innerHTML = `
            <div class="alert alert-info">
                <div class="d-flex align-items-center">
                    <div class="spinner-border spinner-border-sm me-3" role="status"></div>
                    <div>
                        <strong>Processing Contact Matching...</strong><br>
                        <small>Analyzing contacts and finding matches between master and child files...</small>
                    </div>
                </div>
            </div>
        `;

        try {
            const formData = new FormData();
            formData.append('masterFile', masterFileInput.files[0]);
            formData.append('childFile', childFileInput.files[0]);

            const response = await fetch('/api/master-child-notes/match', {
                method: 'POST',
                body: formData
            });

            const data = await response.json();

            if (data.success) {
                matchingResults = data;
                displayMatchingResults(data);
                
                // Enable transfer button
                startTransferBtn.disabled = false;
                
                showAlert('Contact matching completed successfully!', 'success');
            } else {
                showAlert('Contact matching failed: ' + data.error, 'danger');
            }
        } catch (error) {
            showAlert('Contact matching failed: ' + error.message, 'danger');
        }
    });

    // Start Notes Transfer
    startTransferBtn?.addEventListener('click', async function() {
        if (!matchingResults || !matchingResults.matches) {
            showAlert('Please complete contact matching first.', 'warning');
            return;
        }

        const batchSize = document.getElementById('transferBatchSize').value;
        const dryRun = document.getElementById('dryRunMode').checked;
        const processExact = document.getElementById('processExactMatches').checked;
        const processFuzzy = document.getElementById('processFuzzyMatches').checked;

        // Validate match type selection
        if (!processExact && !processFuzzy) {
            showAlert('Please select at least one match type to process (Exact or Fuzzy).', 'warning');
            return;
        }

        // Filter matches based on selected types
        const filteredMatches = matchingResults.matches.filter(match => {
            const matchType = match.match_type;
            if (matchType === 'exact' && processExact) return true;
            if (matchType === 'fuzzy' && processFuzzy) return true;
            return false;
        });

        if (filteredMatches.length === 0) {
            showAlert('No matches found for the selected match types.', 'warning');
            return;
        }

        // Show summary of what will be processed
        const exactCount = filteredMatches.filter(m => m.match_type === 'exact').length;
        const fuzzyCount = filteredMatches.filter(m => m.match_type === 'fuzzy').length;
        const summaryMsg = `Processing ${filteredMatches.length} matches: ${exactCount} exact, ${fuzzyCount} fuzzy`;
        
        // Switch to transfer tab
        document.getElementById('transfer-tab').click();

        try {
            const response = await fetch('/api/master-child-notes/transfer', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    matches: filteredMatches,
                    batch_size: parseInt(batchSize),
                    dry_run: dryRun,
                    match_type_filters: {
                        exact: processExact,
                        fuzzy: processFuzzy
                    }
                })
            });

            const data = await response.json();

            if (data.success) {
                // Start progress tracking
                startTransferProgressTracking(data.processing_id);
                showAlert(`${summaryMsg}${dryRun ? ' (DRY RUN MODE)' : ''}!`, 'info');
            } else {
                showAlert('Notes transfer failed: ' + data.message, 'danger');
            }
        } catch (error) {
            showAlert('Notes transfer failed: ' + error.message, 'danger');
        }
    });

    // Helper Functions
    function readFileAsText(file) {
        return new Promise((resolve, reject) => {
            const reader = new FileReader();
            reader.onload = e => resolve(e.target.result);
            reader.onerror = e => reject(new Error('Failed to read file'));
            reader.readAsText(file);
        });
    }

    function parseCSVPreview(csvContent, fileType) {
        const lines = csvContent.split('\n');
        const headers = lines[0].split(',').map(h => h.trim().replace(/"/g, ''));
        
        // Check required columns
        const requiredColumns = ['Contact Name', 'phone', 'stage', 'Contact ID', 'Account Id'];
        const missingColumns = requiredColumns.filter(col => !headers.includes(col));
        
        let html = `
            <div class="card mt-3">
                <div class="card-header bg-light">
                    <h6 class="mb-0">${fileType} File Preview</h6>
                </div>
                <div class="card-body">
        `;
        
        if (missingColumns.length > 0) {
            html += `
                <div class="alert alert-danger">
                    <strong>Missing Required Columns:</strong> ${missingColumns.join(', ')}
                </div>
            `;
        } else {
            html += `
                <div class="alert alert-success">
                    <strong>✓ All required columns found</strong>
                </div>
            `;
        }
        
        html += `
                    <p><strong>Total Rows:</strong> ${lines.length - 1}</p>
                    <p><strong>Columns Found:</strong> ${headers.join(', ')}</p>
        `;
        
        // Show sample data
        if (lines.length > 1) {
            html += `
                <h6>Sample Data (first 3 rows):</h6>
                <div class="table-responsive">
                    <table class="table table-sm table-striped">
                        <thead>
                            <tr>
                                ${headers.map(h => `<th>${h}</th>`).join('')}
                            </tr>
                        </thead>
                        <tbody>
            `;
            
            for (let i = 1; i <= Math.min(4, lines.length - 1); i++) {
                const values = lines[i].split(',').map(v => v.trim().replace(/"/g, ''));
                html += `<tr>${values.map(v => `<td>${v || '-'}</td>`).join('')}</tr>`;
            }
            
            html += `
                        </tbody>
                    </table>
                </div>
            `;
        }
        
        html += `
                </div>
            </div>
        `;
        
        return html;
    }

    function updateStartMatchingButton() {
        if (masterFileContent && childFileContent) {
            startMatchingBtn.disabled = false;
        }
    }

    function displayMatchingResults(data) {
        const matchingResults = document.getElementById('matching-results');
        const summary = data.summary;
        
        let html = `
            <div class="card">
                <div class="card-header bg-success text-white">
                    <h5 class="mb-0"><i class="fas fa-link"></i> Contact Matching Results</h5>
                </div>
                <div class="card-body">
                    <div class="row">
                        <div class="col-md-6">
                            <div class="card bg-light">
                                <div class="card-body">
                                    <h6><i class="fas fa-chart-pie"></i> Matching Summary</h6>
                                    <ul class="list-unstyled">
                                        <li><strong>Total Master Contacts:</strong> ${summary.total_master_contacts}</li>
                                        <li><strong>Total Child Contacts:</strong> ${summary.total_child_contacts}</li>
                                        <li class="text-success"><strong>Exact Matches:</strong> ${summary.exact_matches}</li>
                                        <li class="text-warning"><strong>Fuzzy Matches:</strong> ${summary.fuzzy_matches}</li>
                                        <li class="text-danger"><strong>No Matches:</strong> ${summary.no_matches}</li>
                                        <li class="text-info"><strong>Multiple Matches:</strong> ${summary.multiple_matches}</li>
                                    </ul>
                                </div>
                            </div>
                        </div>
                        <div class="col-md-6">
                            <div class="card bg-light">
                                <div class="card-body">
                                    <h6><i class="fas fa-percentage"></i> Match Quality</h6>
                                    <div class="progress mb-2">
                                        <div class="progress-bar bg-success" style="width: ${(summary.exact_matches / summary.total_master_contacts * 100).toFixed(1)}%"></div>
                                        <div class="progress-bar bg-warning" style="width: ${(summary.fuzzy_matches / summary.total_master_contacts * 100).toFixed(1)}%"></div>
                                    </div>
                                    <small>
                                        <span class="badge bg-success">${((summary.exact_matches + summary.fuzzy_matches) / summary.total_master_contacts * 100).toFixed(1)}% Total Match Rate</span>
                                    </small>
                                </div>
                            </div>
                        </div>
                    </div>
                    
                    <div class="mt-3">
                        <div class="alert alert-info">
                            <h6><i class="fas fa-filter"></i> Notes Transfer Filtering</h6>
                            <p class="mb-2">In the Transfer tab, you can choose which match types to process:</p>
                            <ul class="mb-0">
                                <li><strong>Exact Matches (${summary.exact_matches}):</strong> <span class="badge bg-success">90%+</span> similarity - highest confidence</li>
                                <li><strong>Fuzzy Matches (${summary.fuzzy_matches}):</strong> <span class="badge bg-warning">70-89%</span> similarity - review recommended</li>
                            </ul>
                        </div>
                    </div>
        `;
        
        // Show sample matches
        if (data.matches && data.matches.length > 0) {
            html += `
                <div class="mt-4">
                    <h6><i class="fas fa-list"></i> Sample Matches (Top 10)</h6>
                    <div class="table-responsive">
                        <table class="table table-sm table-striped">
                            <thead>
                                <tr>
                                    <th>Match Type</th>
                                    <th>Score</th>
                                    <th>Master Contact</th>
                                    <th>Child Contact</th>
                                    <th>Master Phone</th>
                                    <th>Child Phone</th>
                                </tr>
                            </thead>
                            <tbody>
            `;
            
            data.matches.slice(0, 10).forEach(match => {
                const badgeClass = match.match_type === 'exact' ? 'bg-success' : 'bg-warning';
                html += `
                    <tr>
                        <td><span class="badge ${badgeClass}">${match.match_type}</span></td>
                        <td>${(match.match_score * 100).toFixed(1)}%</td>
                        <td>${match.master_contact['Contact Name'] || '-'}</td>
                        <td>${match.child_contact['Contact Name'] || '-'}</td>
                        <td>${match.master_contact.phone || '-'}</td>
                        <td>${match.child_contact.phone || '-'}</td>
                    </tr>
                `;
            });
            
            html += `
                            </tbody>
                        </table>
                    </div>
                </div>
            `;
        }
        
        // Show unmatched contacts if any
        if (data.unmatched_master && data.unmatched_master.length > 0) {
            html += `
                <div class="mt-4">
                    <h6><i class="fas fa-exclamation-triangle"></i> Unmatched Master Contacts (First 10)</h6>
                    <div class="table-responsive">
                        <table class="table table-sm table-striped">
                            <thead>
                                <tr>
                                    <th>Contact Name</th>
                                    <th>Phone</th>
                                    <th>Stage</th>
                                    <th>Contact ID</th>
                                </tr>
                            </thead>
                            <tbody>
            `;
            
            data.unmatched_master.slice(0, 10).forEach(unmatched => {
                const contact = unmatched.master_contact;
                html += `
                    <tr>
                        <td>${contact['Contact Name'] || '-'}</td>
                        <td>${contact.phone || '-'}</td>
                        <td>${contact.stage || '-'}</td>
                        <td>${contact['Contact ID'] || '-'}</td>
                    </tr>
                `;
            });
            
            html += `
                            </tbody>
                        </table>
                    </div>
                </div>
            `;
        }
        
        html += `
                    <div class="mt-4">
                        <button class="btn btn-outline-primary" id="download-matching-results">
                            <i class="fas fa-download"></i> Download Detailed Matching Results
                        </button>
                    </div>
                </div>
            </div>
        `;
        
        matchingResults.innerHTML = html;
        
        // Add download handler
        document.getElementById('download-matching-results')?.addEventListener('click', function() {
            downloadMatchingResults();
        });
    }

    function startTransferProgressTracking(processingId) {
        const progressDiv = document.getElementById('transfer-progress');
        
        progressDiv.innerHTML = `
            <div class="card">
                <div class="card-header bg-info text-white">
                    <h6 class="mb-0"><i class="fas fa-tasks"></i> Notes Transfer Progress</h6>
                </div>
                <div class="card-body">
                    <div class="progress mb-3">
                        <div id="transfer-progress-bar" class="progress-bar progress-bar-striped progress-bar-animated" 
                             role="progressbar" style="width: 0%"></div>
                    </div>
                    <div id="transfer-progress-details"></div>
                </div>
            </div>
        `;
        
        const trackProgress = async () => {
            try {
                const response = await fetch(`/api/master-child-notes/progress/${processingId}`);
                const data = await response.json();
                
                if (data.success) {
                    updateTransferProgress(data.progress);
                    
                    if (data.progress.status === 'completed' || data.progress.status === 'failed') {
                        // Process completed
                        if (data.progress.status === 'completed') {
                            showAlert('Notes transfer completed successfully!', 'success');
                            // Switch to results tab
                            document.getElementById('results-tab').click();
                            displayTransferResults(data.progress);
                        } else {
                            showAlert('Notes transfer failed. Check error details.', 'danger');
                        }
                        return;
                    }
                    
                    // Continue tracking if still processing - more frequent updates
                    setTimeout(trackProgress, 1500); // Check every 1.5 seconds for live updates
                }
            } catch (error) {
                console.error('Progress tracking error:', error);
                setTimeout(trackProgress, 3000); // Retry after longer delay on error
            }
        };
        
        trackProgress();
    }

    function updateTransferProgress(progress) {
        const percentage = Math.round((progress.completed / progress.total) * 100);
        const progressBar = document.getElementById('transfer-progress-bar');
        const progressDetails = document.getElementById('transfer-progress-details');
        
        if (progressBar) {
            progressBar.style.width = percentage + '%';
            progressBar.textContent = percentage + '%';
        }
        
        if (progressDetails) {
            const exactCount = progress.exact_matches_count || 0;
            const fuzzyCount = progress.fuzzy_matches_count || 0;
            
            // Parse status for better display
            let statusDisplay = progress.status;
            if (progress.status.includes('processing_contact_')) {
                const contactMatch = progress.status.match(/processing_contact_(\d+)\/(\d+)/);
                if (contactMatch) {
                    statusDisplay = `Processing contact ${contactMatch[1]} of ${contactMatch[2]}`;
                }
            } else if (progress.status.includes('processing_batch_')) {
                const batchMatch = progress.status.match(/processing_batch_(\d+)/);
                if (batchMatch) {
                    statusDisplay = `Processing batch ${batchMatch[1]}`;
                }
            }
            
            progressDetails.innerHTML = `
                <div class="row">
                    <div class="col-md-6">
                        <strong>Status:</strong> <span class="text-primary">${statusDisplay}</span><br>
                        <strong>Completed:</strong> <span class="badge bg-primary">${progress.completed} / ${progress.total}</span><br>
                        <strong>Success:</strong> <span class="text-success">${progress.success_count}</span><br>
                        <strong>Errors:</strong> <span class="text-danger">${progress.error_count}</span>
                    </div>
                    <div class="col-md-6">
                        <strong>Notes Transferred:</strong> <span class="badge bg-info">${progress.notes_transferred}</span><br>
                        <strong>Current Batch:</strong> ${progress.current_batch || 'N/A'}<br>
                        <strong>Rate:</strong> <span class="text-muted">${progress.rate || 'Calculating...'} pairs/min</span><br>
                        <strong>ETA:</strong> <span class="text-muted">${progress.eta || 'Calculating...'}</span>
                    </div>
                </div>
                ${exactCount > 0 || fuzzyCount > 0 ? `
                    <div class="mt-2">
                        <small class="text-muted">
                            Processing: 
                            ${exactCount > 0 ? `<span class="badge bg-success">${exactCount} Exact</span>` : ''}
                            ${fuzzyCount > 0 ? `<span class="badge bg-warning ms-1">${fuzzyCount} Fuzzy</span>` : ''}
                        </small>
                    </div>
                ` : ''}
                ${progress.recent_errors && progress.recent_errors.length > 0 ? `
                    <div class="mt-3">
                        <strong>Recent Errors:</strong>
                        <ul class="list-unstyled mt-2">
                            ${progress.recent_errors.slice(0, 3).map(error => `<li><small class="text-danger">• ${error}</small></li>`).join('')}
                        </ul>
                    </div>
                ` : ''}
            `;
        }
    }

    function displayTransferResults(progress) {
        const resultsDiv = document.getElementById('results-summary');
        
        const successRate = ((progress.success_count / progress.total) * 100).toFixed(1);
        const exactCount = progress.exact_matches_count || 0;
        const fuzzyCount = progress.fuzzy_matches_count || 0;
        
        resultsDiv.innerHTML = `
            <div class="card">
                <div class="card-header bg-success text-white">
                    <h5 class="mb-0"><i class="fas fa-chart-line"></i> Transfer Results Summary</h5>
                </div>
                <div class="card-body">
                    <div class="row">
                        <div class="col-md-6">
                            <div class="card bg-light">
                                <div class="card-body">
                                    <h6><i class="fas fa-chart-bar"></i> Overall Statistics</h6>
                                    <ul class="list-unstyled">
                                        <li><strong>Total Contact Pairs Processed:</strong> ${progress.total}</li>
                                        <li><strong>Successful Transfers:</strong> ${progress.success_count}</li>
                                        <li><strong>Failed Transfers:</strong> ${progress.error_count}</li>
                                        <li><strong>Total Notes Transferred:</strong> ${progress.notes_transferred}</li>
                                        <li><strong>Success Rate:</strong> ${successRate}%</li>
                                        ${progress.dry_run ? '<li><span class="badge bg-warning">DRY RUN MODE</span></li>' : ''}
                                    </ul>
                                    ${exactCount > 0 || fuzzyCount > 0 ? `
                                        <div class="mt-2">
                                            <small><strong>Match Types Processed:</strong></small><br>
                                            ${exactCount > 0 ? `<span class="badge bg-success me-1">${exactCount} Exact Matches</span>` : ''}
                                            ${fuzzyCount > 0 ? `<span class="badge bg-warning">${fuzzyCount} Fuzzy Matches</span>` : ''}
                                        </div>
                                    ` : ''}
                                </div>
                            </div>
                        </div>
                        <div class="col-md-6">
                            <div class="card bg-light">
                                <div class="card-body">
                                    <h6><i class="fas fa-clock"></i> Timing Information</h6>
                                    <ul class="list-unstyled">
                                        <li><strong>Processing Rate:</strong> ${progress.rate || 'N/A'} pairs/min</li>
                                        <li><strong>Status:</strong> ${progress.status}</li>
                                    </ul>
                                    <div class="progress">
                                        <div class="progress-bar bg-success" style="width: ${successRate}%"></div>
                                    </div>
                                    <small class="text-muted">${successRate}% Success Rate</small>
                                </div>
                            </div>
                        </div>
                    </div>
                    
                    ${progress.recent_errors && progress.recent_errors.length > 0 ? `
                        <div class="mt-4">
                            <h6><i class="fas fa-exclamation-triangle"></i> Error Details</h6>
                            <div class="alert alert-warning">
                                <ul class="mb-0">
                                    ${progress.recent_errors.map(error => `<li>${error}</li>`).join('')}
                                </ul>
                            </div>
                        </div>
                    ` : ''}
                </div>
            </div>
        `;
    }

    async function downloadMatchingResults() {
        try {
            const response = await fetch('/api/master-child-notes/download-matches', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json'
                },
                body: JSON.stringify({
                    matches: matchingResults.matches,
                    unmatched: matchingResults.unmatched_master
                })
            });
            
            const blob = await response.blob();
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `master-child-matching-results-${new Date().toISOString().split('T')[0]}.csv`;
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            document.body.removeChild(a);
        } catch (error) {
            showAlert('Failed to download matching results: ' + error.message, 'danger');
        }
    }

    function showAlert(message, type) {
        // Create alert in the active tab
        const activePane = document.querySelector('.tab-pane.active');
        if (activePane) {
            const existingAlert = activePane.querySelector('.alert-dismissible');
            if (existingAlert) {
                existingAlert.remove();
            }
            
            const alertHtml = `
                <div class="alert alert-${type} alert-dismissible fade show" role="alert">
                    ${message}
                    <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
                </div>
            `;
            
            activePane.insertAdjacentHTML('afterbegin', alertHtml);
        }
    }

    // Initialize Bootstrap tabs
    const triggerTabList = [].slice.call(document.querySelectorAll('#masterChildTabs button'));
    triggerTabList.forEach(function (triggerEl) {
        new bootstrap.Tab(triggerEl);
    });
});
