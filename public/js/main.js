document.addEventListener('DOMContentLoaded', function() {
  // Bootstrap tooltips
  const tooltipTriggerList = document.querySelectorAll('[data-bs-toggle="tooltip"]');
  if (tooltipTriggerList.length > 0) {
    const tooltipList = [...tooltipTriggerList].map(tooltipTriggerEl => new bootstrap.Tooltip(tooltipTriggerEl));
  }
  
  // Mobile sidebar toggle
  const sidebarToggle = document.querySelector('.navbar-toggler');
  if (sidebarToggle) {
    sidebarToggle.addEventListener('click', function() {
      document.querySelector('.sidebar').classList.toggle('d-md-block');
    });
  }
  
  // Form validation
  const forms = document.querySelectorAll('.needs-validation');
  Array.from(forms).forEach(form => {
    form.addEventListener('submit', event => {
      if (!form.checkValidity()) {
        event.preventDefault();
        event.stopPropagation();
      }
      form.classList.add('was-validated');
    }, false);
  });
  
  // Settings page calculations
  function calculateValues() {
    const amount = parseFloat(document.getElementById('amount_per_trade')?.value || 0);
    const leverage = parseFloat(document.getElementById('leverage')?.value || 0);
    const stopLoss = parseFloat(document.getElementById('stop_loss_percent')?.value || 0);
    const profit1 = parseFloat(document.getElementById('profit_percent_1')?.value || 0);
    const profit2 = parseFloat(document.getElementById('profit_percent_2')?.value || 0);
    
    if (amount <= 0 || leverage <= 0 || stopLoss <= 0) {
      return;
    }
    
    // Calculate values
    const leveragedAmount = amount * leverage;
    const adjustedStopLoss = stopLoss / leverage;
    const adjustedTakeProfit1 = profit1 / leverage;
    const adjustedTakeProfit2 = profit2 > 0 ? profit2 / leverage : 0;
    
    // Update DOM
    document.getElementById('leveraged_amount')?.textContent = leveragedAmount.toFixed(2) + ' USDT';
    document.getElementById('adjusted_stop_loss')?.textContent = adjustedStopLoss.toFixed(4) + ' %';
    document.getElementById('adjusted_take_profit_1')?.textContent = adjustedTakeProfit1.toFixed(4) + ' %';
    document.getElementById('adjusted_take_profit_2')?.textContent = adjustedTakeProfit2.toFixed(4) + ' %';
  }
  
  // Add event listeners to settings inputs if they exist
  const settingsInputs = [
    'amount_per_trade',
    'leverage',
    'stop_loss_percent',
    'profit_percent_1',
    'profit_percent_2'
  ];
  
  let hasSettingsInputs = false;
  
  settingsInputs.forEach(id => {
    const input = document.getElementById(id);
    if (input) {
      hasSettingsInputs = true;
      input.addEventListener('input', calculateValues);
    }
  });
  
  // Calculate initial values if on settings page
  if (hasSettingsInputs) {
    calculateValues();
  }
  
  // Copy license key to clipboard
  const licenseKeyCopyButtons = document.querySelectorAll('.copy-key');
  licenseKeyCopyButtons.forEach(button => {
    button.addEventListener('click', function() {
      const key = this.getAttribute('data-key');
      navigator.clipboard.writeText(key)
        .then(() => {
          this.textContent = 'Kopyalandı!';
          setTimeout(() => {
            this.textContent = 'Kopyala';
          }, 2000);
        })
        .catch(err => {
          console.error('Clipboard write failed:', err);
        });
    });
  });
});