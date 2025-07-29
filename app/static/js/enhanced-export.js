// Enhanced Export JavaScript functionality
document.addEventListener('DOMContentLoaded', function() {
    const enhancedExportBtn = document.getElementById('enhanced-export-btn');
    const enhancedExportWorkflow = document.getElementById('enhanced-export-workflow');
    const enhancedSubaccountList = document.getElementById('enhanced-subaccount-list');
    const enhancedExportSubmitBtn = document.getElementById('enhanced-export-submit-btn');
    const enhancedExportStatus = document.getElementById('enhanced-export-status');

    if (enhancedExportBtn) {
        enhancedExportBtn.addEventListener('click', async function() {
            enhancedExportWorkflow.style.display = 'block';
            this.style.display = 'none';
            
            // Load subaccounts
            try {
                const response = await fetch('/api/v1/enhanced/enhanced-subaccounts');
                const subaccounts = await response.json();
                
                enhancedSubaccountList.innerHTML = '';
                subaccounts.forEach(subaccount => {
                    const div = document.createElement('div');
                    div.className = 'form-check mb-3 p-3 border rounded';
                    div.innerHTML = `
                        <input class="form-check-input subaccount-checkbox" type="checkbox" value="${subaccount.id}" id="enhanced-sub-${subaccount.id}">
                        <label class="form-check-label fw-bold" for="enhanced-sub-${subaccount.id}">
                            <i class="fas fa-building"></i> ${subaccount.name} (${subaccount.id})
                        </label>
                        <div class="pipeline-selection mt-2" id="enhanced-pipelines-${subaccount.id}" style="display:none;">
                            <div class="text-muted"><i class="fas fa-spinner fa-spin"></i> Loading pipelines...</div>
                        </div>
                    `;
                    enhancedSubaccountList.appendChild(div);
                    
                    // Add event listener for checkbox
                    const checkbox = div.querySelector('.subaccount-checkbox');
                    checkbox.addEventListener('change', async function() {
                        const pipelineDiv = document.getElementById(`enhanced-pipelines-${subaccount.id}`);
                        
                        if (this.checked) {
                            pipelineDiv.style.display = 'block';
                            try {
                                const pipelineResponse = await fetch(`/api/v1/enhanced/enhanced-pipelines/${subaccount.id}`);
                                const pipelines = await pipelineResponse.json();
                                
                                let pipelineHtml = '<div class="ms-3 mt-2"><strong><i class="fas fa-sitemap"></i> Select Pipelines:</strong><br>';
                                pipelines.forEach(pipeline => {
                                    pipelineHtml += `
                                        <div class="form-check mt-1">
                                            <input class="form-check-input pipeline-checkbox" type="checkbox" value="${pipeline.id}" id="enhanced-pipeline-${pipeline.id}">
                                            <label class="form-check-label" for="enhanced-pipeline-${pipeline.id}">
                                                <i class="fas fa-arrow-right"></i> ${pipeline.name}
                                            </label>
                                        </div>
                                    `;
                                });
                                pipelineHtml += '</div>';
                                pipelineDiv.innerHTML = pipelineHtml;
                                
                                // Add event listeners to pipeline checkboxes
                                pipelineDiv.querySelectorAll('.pipeline-checkbox').forEach(pipelineCheckbox => {
                                    pipelineCheckbox.addEventListener('change', checkEnhancedExportButton);
                                });
                            } catch (error) {
                                pipelineDiv.innerHTML = '<div class="text-danger"><i class="fas fa-exclamation-triangle"></i> Error loading pipelines</div>';
                            }
                        } else {
                            pipelineDiv.style.display = 'none';
                            pipelineDiv.innerHTML = '<div class="text-muted"><i class="fas fa-spinner fa-spin"></i> Loading pipelines...</div>';
                        }
                        checkEnhancedExportButton();
                    });
                });
            } catch (error) {
                enhancedSubaccountList.innerHTML = '<div class="alert alert-danger"><i class="fas fa-exclamation-triangle"></i> Error loading subaccounts</div>';
            }
        });
    }

    function checkEnhancedExportButton() {
        const checkedSubaccounts = document.querySelectorAll('.subaccount-checkbox:checked');
        let hasValidSelection = false;
        
        checkedSubaccounts.forEach(subaccountCheckbox => {
            const subaccountId = subaccountCheckbox.value;
            const checkedPipelines = document.querySelectorAll(`#enhanced-pipelines-${subaccountId} .pipeline-checkbox:checked`);
            if (checkedPipelines.length > 0) {
                hasValidSelection = true;
            }
        });
        
        if (enhancedExportSubmitBtn) {
            enhancedExportSubmitBtn.disabled = !hasValidSelection;
        }
    }

    if (enhancedExportSubmitBtn) {
        enhancedExportSubmitBtn.addEventListener('click', async function() {
            const selections = [];
            const checkedSubaccounts = document.querySelectorAll('.subaccount-checkbox:checked');
            
            // Get subaccounts data
            const subaccountsResponse = await fetch('/api/v1/enhanced/enhanced-subaccounts');
            const subaccounts = await subaccountsResponse.json();
            
            checkedSubaccounts.forEach(subaccountCheckbox => {
                const subaccountId = subaccountCheckbox.value;
                const subaccount = subaccounts.find(s => s.id === subaccountId);
                const checkedPipelines = document.querySelectorAll(`#enhanced-pipelines-${subaccountId} .pipeline-checkbox:checked`);
                
                if (checkedPipelines.length > 0) {
                    const pipelines = Array.from(checkedPipelines).map(cb => cb.value);
                    selections.push({
                        account_id: subaccountId,
                        api_key: subaccount.api_key,
                        pipelines: pipelines
                    });
                }
            });

            if (selections.length === 0) {
                alert('Please select at least one subaccount with pipelines');
                return;
            }

            // Disable button and show progress
            enhancedExportSubmitBtn.disabled = true;
            enhancedExportStatus.innerHTML = `
                <div class="alert alert-info">
                    <div class="d-flex align-items-center">
                        <div class="spinner-border spinner-border-sm me-3" role="status"></div>
                        <div>
                            <strong><i class="fas fa-cogs"></i> Processing Enhanced Export...</strong><br>
                            <small>Step 1: Fetching opportunities...<br>
                            Step 2: Getting detailed contact information...<br>
                            Step 3: Retrieving custom fields...<br>
                            <em>This may take several minutes depending on the number of contacts.</em></small>
                        </div>
                    </div>
                </div>
            `;

            try {
                const response = await fetch('/api/v1/enhanced/enhanced-export', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json'
                    },
                    body: JSON.stringify({
                        selections: selections
                    })
                });

                if (response.ok) {
                    const blob = await response.blob();
                    const url = window.URL.createObjectURL(blob);
                    const a = document.createElement('a');
                    a.href = url;
                    a.download = 'Enhanced_GHL_Opportunities_Export.xlsx';
                    a.click();
                    window.URL.revokeObjectURL(url);
                    
                    enhancedExportStatus.innerHTML = `
                        <div class="alert alert-success">
                            <h6><i class="fas fa-check-circle"></i> Enhanced Export Completed Successfully!</h6>
                            <p class="mb-0">Your enhanced export with detailed contact information and custom fields has been downloaded.</p>
                        </div>
                    `;
                } else {
                    const errorText = await response.text();
                    throw new Error(`Export failed: ${errorText}`);
                }
            } catch (error) {
                console.error('Enhanced export error:', error);
                enhancedExportStatus.innerHTML = `
                    <div class="alert alert-danger">
                        <h6><i class="fas fa-exclamation-triangle"></i> Enhanced Export Failed</h6>
                        <p class="mb-0">Error: ${error.message}</p>
                        <small>Please try again or contact support if the issue persists.</small>
                    </div>
                `;
            } finally {
                // Re-enable button
                enhancedExportSubmitBtn.disabled = false;
            }
        });
    }

    // Debug Custom Fields functionality
    const debugCustomFieldsBtn = document.getElementById('debug-custom-fields-btn');
    if (debugCustomFieldsBtn) {
        debugCustomFieldsBtn.addEventListener('click', async function() {
            const checkedSubaccounts = document.querySelectorAll('.subaccount-checkbox:checked');
            
            if (checkedSubaccounts.length === 0) {
                alert('Please select at least one subaccount to debug custom fields.');
                return;
            }

            // Show loading state
            const debugResults = document.getElementById('debug-results');
            debugResults.innerHTML = `
                <div class="card">
                    <div class="card-header bg-info text-white">
                        <h5 class="mb-0"><i class="fas fa-bug"></i> Custom Fields Debug Results</h5>
                    </div>
                    <div class="card-body">
                        <div class="alert alert-info">
                            <div class="d-flex align-items-center">
                                <div class="spinner-border spinner-border-sm me-3" role="status"></div>
                                <div>Debugging custom fields for selected subaccounts...</div>
                            </div>
                        </div>
                    </div>
                </div>
            `;

            try {
                // Debug each selected subaccount
                const debugPromises = Array.from(checkedSubaccounts).map(async (checkbox) => {
                    const subaccountId = checkbox.value;
                    const response = await fetch(`/api/v1/enhanced/debug-custom-fields/${subaccountId}?limit=10`);
                    const data = await response.json();
                    return { subaccountId, data };
                });

                const results = await Promise.all(debugPromises);
                
                // Display results
                let resultsHtml = `
                    <div class="card">
                        <div class="card-header bg-info text-white">
                            <h5 class="mb-0"><i class="fas fa-bug"></i> Custom Fields Debug Results</h5>
                        </div>
                        <div class="card-body">
                `;

                results.forEach(({ subaccountId, data }) => {
                    const summary = data.summary || {};
                    const targetMatches = data.target_field_matches || [];
                    const contactsWithFields = data.contacts_with_target_fields || [];

                    resultsHtml += `
                        <div class="mb-4">
                            <h6><i class="fas fa-building"></i> Subaccount: ${subaccountId}</h6>
                            
                            <div class="row">
                                <div class="col-md-6">
                                    <div class="card bg-light">
                                        <div class="card-body">
                                            <h6><i class="fas fa-chart-bar"></i> Summary</h6>
                                            <ul class="list-unstyled">
                                                <li><strong>Total Custom Field Definitions:</strong> ${summary.total_custom_field_definitions || 0}</li>
                                                <li><strong>Target Field Matches Found:</strong> ${summary.target_field_matches_in_definitions || 0}</li>
                                                <li><strong>Contacts Checked:</strong> ${summary.contacts_checked || 0}</li>
                                                <li><strong>Contacts with Target Fields:</strong> ${summary.contacts_with_target_fields || 0}</li>
                                            </ul>
                                        </div>
                                    </div>
                                </div>
                                
                                <div class="col-md-6">
                                    <div class="card bg-light">
                                        <div class="card-body">
                                            <h6><i class="fas fa-crosshairs"></i> Target Field Matches</h6>
                                            ${targetMatches.length > 0 ? 
                                                targetMatches.map(match => 
                                                    `<div class="badge bg-success me-1 mb-1">${match.name}</div>`
                                                ).join('') : 
                                                '<em class="text-muted">No target fields found in definitions</em>'
                                            }
                                        </div>
                                    </div>
                                </div>
                            </div>

                            ${contactsWithFields.length > 0 ? `
                                <div class="mt-3">
                                    <h6><i class="fas fa-users"></i> Contacts with Target Custom Fields</h6>
                                    <div class="table-responsive">
                                        <table class="table table-sm table-striped">
                                            <thead>
                                                <tr>
                                                    <th>Contact Name</th>
                                                    <th>Contact ID</th>
                                                    <th>Target Fields Found</th>
                                                </tr>
                                            </thead>
                                            <tbody>
                                                ${contactsWithFields.map(contact => `
                                                    <tr>
                                                        <td>${contact.name}</td>
                                                        <td><code>${contact.id}</code></td>
                                                        <td>
                                                            ${contact.target_fields_found.map(field => 
                                                                `<div class="badge bg-primary me-1 mb-1">${field.field_name}: ${field.value}</div>`
                                                            ).join('')}
                                                        </td>
                                                    </tr>
                                                `).join('')}
                                            </tbody>
                                        </table>
                                    </div>
                                </div>
                            ` : `
                                <div class="mt-3">
                                    <div class="alert alert-warning">
                                        <i class="fas fa-exclamation-triangle"></i> 
                                        No contacts found with the target custom fields populated. 
                                        This could mean:
                                        <ul class="mb-0 mt-2">
                                            <li>The custom fields exist but are not populated for the sampled contacts</li>
                                            <li>The custom fields are named differently than expected</li>
                                            <li>The contacts with these fields are not in the first 10 contacts</li>
                                        </ul>
                                    </div>
                                </div>
                            `}
                            
                            <hr>
                        </div>
                    `;
                });

                resultsHtml += `
                        </div>
                    </div>
                `;

                debugResults.innerHTML = resultsHtml;

            } catch (error) {
                debugResults.innerHTML = `
                    <div class="card">
                        <div class="card-header bg-danger text-white">
                            <h5 class="mb-0"><i class="fas fa-exclamation-triangle"></i> Debug Error</h5>
                        </div>
                        <div class="card-body">
                            <div class="alert alert-danger">
                                <strong>Error:</strong> ${error.message}
                            </div>
                        </div>
                    </div>
                `;
            }
        });
    }

    // Update button states when subaccounts are selected
    function updateButtonStates() {
        const checkedSubaccounts = document.querySelectorAll('.subaccount-checkbox:checked');
        let hasValidSelection = false;
        
        checkedSubaccounts.forEach(subaccountCheckbox => {
            const subaccountId = subaccountCheckbox.value;
            const checkedPipelines = document.querySelectorAll(`#enhanced-pipelines-${subaccountId} .pipeline-checkbox:checked`);
            if (checkedPipelines.length > 0) {
                hasValidSelection = true;
            }
        });
        
        if (enhancedExportSubmitBtn) {
            enhancedExportSubmitBtn.disabled = !hasValidSelection;
        }
        
        const debugCustomFieldsBtn = document.getElementById('debug-custom-fields-btn');
        if (debugCustomFieldsBtn) {
            debugCustomFieldsBtn.disabled = checkedSubaccounts.length === 0;
        }
    }

    // Initial button state update
    updateButtonStates();
});

// Utility function to show progress updates
function updateEnhancedExportProgress(step, message) {
    const statusDiv = document.getElementById('enhanced-export-status');
    if (statusDiv) {
        statusDiv.innerHTML = `
            <div class="alert alert-info">
                <div class="d-flex align-items-center">
                    <div class="spinner-border spinner-border-sm me-3" role="status"></div>
                    <div>
                        <strong><i class="fas fa-cogs"></i> ${step}</strong><br>
                        <small>${message}</small>
                    </div>
                </div>
            </div>
        `;
    }
}
