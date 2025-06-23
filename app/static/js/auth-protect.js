// This script ensures the Firebase ID token is sent as Authorization header for all navigation to protected pages
function setAuthHeaderAndRedirect(path) {
    firebase.auth().currentUser.getIdToken().then(function(token) {
        // Use fetch to get the page with Authorization header
        fetch(path, {
            headers: { 'Authorization': 'Bearer ' + token }
        })
        .then(response => {
            if (response.redirected) {
                window.location.href = response.url;
            } else if (response.ok) {
                response.text().then(html => {
                    document.open();
                    document.write(html);
                    document.close();
                });
            } else {
                window.location.href = '/login';
            }
        });
    });
}

// On index.html, override card click to use this function
window.addEventListener('DOMContentLoaded', function() {
    const cards = document.querySelectorAll('.automation-card');
    cards.forEach(card => {
        card.onclick = function(e) {
            e.preventDefault();
            setAuthHeaderAndRedirect('/dashboard');
        };
    });
});
