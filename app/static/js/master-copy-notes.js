// Master Copy Notes JavaScript functionality
document.addEventListener('DOMContentLoaded', function() {
    const masterCsvForm = document.getElementById('master-csv-upload-form');
    const statusDiv = document.getElementById('master-upload-status');
    const progressContainer = document.getElementById('master-progress-container');
    const progressBar = document.getElementById('master-progress-bar');
    const progressDetails = document.getElementById('master-progress-details');
    const validationResults = document.getElementById('validation-results');

    // Form submission handler
    if (masterCsvForm) {
        masterCsvForm.addEventListener('submit', async function(e) {
            e.preventDefault();
            
            const fileInput = document.getElementById('masterCsvFile');
            const enableBackup = document.getElementById('enableBackup').checked;
            const enableValidation = document.getElementById('enableValidation').checked;
            const batchSize = document.getElementById('batchSize').value;
            const operationName = document.getElementById('operationName').value;
            
            if (!fileInput.files.length) {
                showAlert('Please select a CSV file.', 'warning');
                return;
            }

            const formData = new FormData();
            formData.append('csvFile', fileInput.files[0]);
            formData.append('enableBackup', enableBackup);
            formData.append('enableValidation', enableValidation);
            formData.append('batchSize', batchSize);
            formData.append('operationName', operationName);

            // Show initial processing status
            showProcessingStatus('Initializing master copy process...');
            
            try {
                const response = await fetch('/api/master-copy-notes', {
                    method: 'POST',
                    body: formData
                });

                const data = await response.json();
                
                if (data.success) {
                    showAlert(data.message, 'success');
                    
                    // Show validation results if available
                    if (data.validation) {
                        displayValidationResults(data.validation);
                    }
                    
                    // Start progress tracking if processing is ongoing
                    if (data.processing_id) {
                        startProgressTracking(data.processing_id);
                    }
                } else {
                    showAlert(data.message || 'Operation failed.', 'danger');
                }
            } catch (error) {
                showAlert('Upload failed: ' + error.message, 'danger');
            }
        });
    }

    // Progress tracking function
    function startProgressTracking(processingId) {
        progressContainer.style.display = 'block';
        
        const trackProgress = async () => {
            try {
                const response = await fetch(`/api/master-copy-notes/progress/${processingId}`);
                const data = await response.json();
                
                if (data.success) {
                    updateProgress(data.progress);
                    
                    if (data.progress.status === 'completed' || data.progress.status === 'failed') {
                        // Process completed
                        if (data.progress.status === 'completed') {
                            showAlert('Master copy process completed successfully!', 'success');
                        } else {
                            showAlert('Master copy process failed. Check error details.', 'danger');
                        }
                        return;
                    }
                    
                    // Continue tracking if still processing
                    setTimeout(trackProgress, 2000);
                }
            } catch (error) {
                console.error('Progress tracking error:', error);
                setTimeout(trackProgress, 5000); // Retry after longer delay
            }
        };
        
        trackProgress();
    }

    // Update progress display
    function updateProgress(progress) {
        const percentage = Math.round((progress.completed / progress.total) * 100);
        
        progressBar.style.width = percentage + '%';
        progressBar.setAttribute('aria-valuenow', percentage);
        progressBar.textContent = percentage + '%';
        
        progressDetails.innerHTML = `
            <div class="row">
                <div class="col-md-6">
                    <strong>Status:</strong> ${progress.status}<br>
                    <strong>Completed:</strong> ${progress.completed} / ${progress.total}<br>
                    <strong>Success:</strong> ${progress.success_count}<br>
                    <strong>Errors:</strong> ${progress.error_count}
                </div>
                <div class="col-md-6">
                    <strong>Current Batch:</strong> ${progress.current_batch || 'N/A'}<br>
                    <strong>Estimated Time Remaining:</strong> ${progress.eta || 'Calculating...'}<br>
                    <strong>Rate:</strong> ${progress.rate || 'N/A'} items/min
                </div>
            </div>
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

    // Display validation results
    function displayValidationResults(validation) {
        const validationTab = document.getElementById('validate-tab');
        const validationPane = document.getElementById('validate-pane');
        
        // Switch to validation tab
        validationTab.click();
        
        let validationHtml = '';
        
        if (validation.is_valid) {
            validationHtml = `
                <div class="alert alert-success">
                    <h6><i class="fas fa-check-circle"></i> Validation Passed</h6>
                    <p class="mb-0">Your CSV file is valid and ready for processing.</p>
                </div>
            `;
        } else {
            validationHtml = `
                <div class="alert alert-danger">
                    <h6><i class="fas fa-exclamation-triangle"></i> Validation Failed</h6>
                    <p>Please fix the following issues before proceeding:</p>
                </div>
            `;
        }
        
        // Add validation details
        validationHtml += `
            <div class="row">
                <div class="col-md-6">
                    <div class="card">
                        <div class="card-header bg-light">
                            <h6 class="mb-0">File Statistics</h6>
                        </div>
                        <div class="card-body">
                            <ul class="list-unstyled">
                                <li><strong>Total Rows:</strong> ${validation.total_rows}</li>
                                <li><strong>Valid Rows:</strong> ${validation.valid_rows}</li>
                                <li><strong>Invalid Rows:</strong> ${validation.invalid_rows}</li>
                                <li><strong>Empty Notes:</strong> ${validation.empty_notes}</li>
                            </ul>
                        </div>
                    </div>
                </div>
                <div class="col-md-6">
                    <div class="card">
                        <div class="card-header bg-light">
                            <h6 class="mb-0">Column Validation</h6>
                        </div>
                        <div class="card-body">
                            <ul class="list-unstyled">
                                ${validation.required_columns.map(col => 
                                    `<li><i class="fas fa-${validation.missing_columns.includes(col) ? 'times text-danger' : 'check text-success'}"></i> ${col}</li>`
                                ).join('')}
                            </ul>
                        </div>
                    </div>
                </div>
            </div>
        `;
        
        if (validation.errors && validation.errors.length > 0) {
            validationHtml += `
                <div class="card mt-3">
                    <div class="card-header bg-danger text-white">
                        <h6 class="mb-0">Validation Errors</h6>
                    </div>
                    <div class="card-body">
                        <ul class="list-group list-group-flush">
                            ${validation.errors.slice(0, 10).map(error => 
                                `<li class="list-group-item text-danger"><small>${error}</small></li>`
                            ).join('')}
                        </ul>
                        ${validation.errors.length > 10 ? 
                            `<p class="mt-2 text-muted"><small>... and ${validation.errors.length - 10} more errors</small></p>` : 
                            ''
                        }
                    </div>
                </div>
            `;
        }
        
        validationResults.innerHTML = validationHtml;
    }

    // Utility functions
    function showAlert(message, type) {
        statusDiv.innerHTML = `
            <div class="alert alert-${type} alert-dismissible fade show" role="alert">
                ${message}
                <button type="button" class="btn-close" data-bs-dismiss="alert"></button>
            </div>
        `;
    }

    function showProcessingStatus(message) {
        statusDiv.innerHTML = `
            <div class="alert alert-info">
                <div class="d-flex align-items-center">
                    <div class="spinner-border spinner-border-sm me-3" role="status"></div>
                    <div>${message}</div>
                </div>
            </div>
        `;
    }

    // Template download handlers
    document.getElementById('download-basic-template')?.addEventListener('click', function(e) {
        e.preventDefault();
        downloadTemplate('basic');
    });

    document.getElementById('download-advanced-template')?.addEventListener('click', function(e) {
        e.preventDefault();
        downloadTemplate('advanced');
    });

    document.getElementById('download-sample-data')?.addEventListener('click', function(e) {
        e.preventDefault();
        downloadTemplate('sample');
    });

    async function downloadTemplate(type) {
        try {
            const response = await fetch(`/api/master-copy-notes/template/${type}`);
            const blob = await response.blob();
            
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = `master-copy-notes-${type}-template.csv`;
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            document.body.removeChild(a);
        } catch (error) {
            showAlert(`Failed to download ${type} template: ${error.message}`, 'danger');
        }
    }

    // Backup management
    document.getElementById('create-manual-backup-btn')?.addEventListener('click', async function() {
        try {
            const response = await fetch('/api/master-copy-notes/backup', {
                method: 'POST'
            });
            const data = await response.json();
            
            if (data.success) {
                showAlert('Manual backup created successfully.', 'success');
                loadBackupList();
            } else {
                showAlert('Failed to create backup: ' + data.message, 'danger');
            }
        } catch (error) {
            showAlert('Backup creation failed: ' + error.message, 'danger');
        }
    });

    // Load backup list
    async function loadBackupList() {
        try {
            const response = await fetch('/api/master-copy-notes/backups');
            const data = await response.json();
            
            const backupList = document.getElementById('backup-list');
            if (data.success && data.backups.length > 0) {
                let html = '<div class="list-group">';
                data.backups.forEach(backup => {
                    html += `
                        <div class="list-group-item">
                            <div class="d-flex w-100 justify-content-between">
                                <h6 class="mb-1">${backup.name}</h6>
                                <small>${backup.created_at}</small>
                            </div>
                            <p class="mb-1">${backup.description}</p>
                            <small>Size: ${backup.size} | Records: ${backup.record_count}</small>
                            <div class="mt-2">
                                <button class="btn btn-sm btn-outline-primary me-2" onclick="restoreBackup('${backup.id}')">
                                    <i class="fas fa-undo"></i> Restore
                                </button>
                                <button class="btn btn-sm btn-outline-success me-2" onclick="downloadBackup('${backup.id}')">
                                    <i class="fas fa-download"></i> Download
                                </button>
                                <button class="btn btn-sm btn-outline-danger" onclick="deleteBackup('${backup.id}')">
                                    <i class="fas fa-trash"></i> Delete
                                </button>
                            </div>
                        </div>
                    `;
                });
                html += '</div>';
                backupList.innerHTML = html;
            } else {
                backupList.innerHTML = `
                    <div class="alert alert-info">
                        <i class="fas fa-info-circle"></i> No backups found.
                    </div>
                `;
            }
        } catch (error) {
            console.error('Failed to load backup list:', error);
        }
    }

    // Load operation history
    async function loadOperationHistory() {
        try {
            const response = await fetch('/api/master-copy-notes/history');
            const data = await response.json();
            
            const historyDiv = document.getElementById('operation-history');
            if (data.success && data.operations.length > 0) {
                let html = '<div class="list-group">';
                data.operations.forEach(op => {
                    const statusClass = op.status === 'completed' ? 'success' : 
                                       op.status === 'failed' ? 'danger' : 'warning';
                    html += `
                        <div class="list-group-item">
                            <div class="d-flex w-100 justify-content-between">
                                <h6 class="mb-1">${op.name || 'Unnamed Operation'}</h6>
                                <span class="badge bg-${statusClass}">${op.status}</span>
                            </div>
                            <p class="mb-1">Records: ${op.total_records} | Success: ${op.success_count} | Errors: ${op.error_count}</p>
                            <small>${op.created_at}</small>
                        </div>
                    `;
                });
                html += '</div>';
                historyDiv.innerHTML = html;
            } else {
                historyDiv.innerHTML = `
                    <div class="alert alert-info">
                        <i class="fas fa-info-circle"></i> No operation history found.
                    </div>
                `;
            }
        } catch (error) {
            console.error('Failed to load operation history:', error);
        }
    }

    // Load backup list and operation history when tabs are activated
    document.getElementById('backup-tab')?.addEventListener('shown.bs.tab', function() {
        loadBackupList();
        loadOperationHistory();
    });

    // Initialize Bootstrap tabs
    const triggerTabList = [].slice.call(document.querySelectorAll('#masterCopyTabs button'));
    triggerTabList.forEach(function (triggerEl) {
        new bootstrap.Tab(triggerEl);
    });
});

// Global functions for backup management
window.restoreBackup = async function(backupId) {
    if (!confirm('Are you sure you want to restore this backup? This will overwrite current data.')) {
        return;
    }
    
    try {
        const response = await fetch(`/api/master-copy-notes/backup/${backupId}/restore`, {
            method: 'POST'
        });
        const data = await response.json();
        
        if (data.success) {
            alert('Backup restored successfully.');
        } else {
            alert('Failed to restore backup: ' + data.message);
        }
    } catch (error) {
        alert('Restore failed: ' + error.message);
    }
};

window.downloadBackup = async function(backupId) {
    try {
        const response = await fetch(`/api/master-copy-notes/backup/${backupId}/download`);
        const blob = await response.blob();
        
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `backup-${backupId}.csv`;
        document.body.appendChild(a);
        a.click();
        window.URL.revokeObjectURL(url);
        document.body.removeChild(a);
    } catch (error) {
        alert('Download failed: ' + error.message);
    }
};

window.deleteBackup = async function(backupId) {
    if (!confirm('Are you sure you want to delete this backup? This action cannot be undone.')) {
        return;
    }
    
    try {
        const response = await fetch(`/api/master-copy-notes/backup/${backupId}`, {
            method: 'DELETE'
        });
        const data = await response.json();
        
        if (data.success) {
            alert('Backup deleted successfully.');
            // Reload backup list
            const backupTab = document.getElementById('backup-tab');
            if (backupTab.classList.contains('active')) {
                loadBackupList();
            }
        } else {
            alert('Failed to delete backup: ' + data.message);
        }
    } catch (error) {
        alert('Delete failed: ' + error.message);
    }
};
