// movies/static/js/stats.js

Chart.defaults.color = '#e0e0e0';
Chart.defaults.borderColor = '#333';

document.addEventListener('DOMContentLoaded', function() {
    // On vérifie que les données sont bien présentes
    if (!window.statsData) {
        console.error("Aucune donnée statistique trouvée.");
        return;
    }

    const data = window.statsData;

    // 1. Graphique Genres (Bar)
    const ctxGenres = document.getElementById('genresChart');
    if (ctxGenres) {
        new Chart(ctxGenres.getContext('2d'), {
            type: 'bar',
            data: {
                labels: data.genres.labels,
                datasets: [{
                    label: 'Nombre de films',
                    data: data.genres.counts,
                    backgroundColor: 'rgba(54, 162, 235, 0.6)',
                    borderColor: 'rgba(54, 162, 235, 1)',
                    borderWidth: 1
                }]
            },
            options: { scales: { y: { beginAtZero: true } } }
        });
    }

    // 2. Graphique Décennies (Line)
    const ctxDecades = document.getElementById('decadesChart');
    if (ctxDecades) {
        new Chart(ctxDecades.getContext('2d'), {
            type: 'line',
            data: {
                labels: data.decades.labels,
                datasets: [{
                    label: 'Production de films',
                    data: data.decades.counts,
                    fill: true,
                    backgroundColor: 'rgba(75, 192, 192, 0.2)',
                    borderColor: 'rgba(75, 192, 192, 1)',
                    tension: 0.3
                }]
            }
        });
    }

    // 3. Graphique Acteurs (Horizontal Bar)
    const ctxActors = document.getElementById('actorsChart');
    if (ctxActors) {
        new Chart(ctxActors.getContext('2d'), {
            type: 'bar',
            data: {
                labels: data.actors.labels,
                datasets: [{
                    label: 'Films joués',
                    data: data.actors.counts,
                    backgroundColor: 'rgba(255, 159, 64, 0.6)',
                    borderColor: 'rgba(255, 159, 64, 1)',
                    borderWidth: 1
                }]
            },
            options: {
                indexAxis: 'y',
                scales: { x: { beginAtZero: true } }
            }
        });
    }

    // 4. Graphique Notes (Histogramme)
    const ctxRatings = document.getElementById('ratingsChart');
    if (ctxRatings) {
        new Chart(ctxRatings.getContext('2d'), {
            type: 'bar',
            data: {
                labels: data.ratings.labels,
                datasets: [{
                    label: 'Nombre de films',
                    data: data.ratings.counts,
                    backgroundColor: 'rgba(153, 102, 255, 0.6)',
                    borderColor: 'rgba(153, 102, 255, 1)',
                    borderWidth: 1
                }]
            },
            options: { scales: { y: { beginAtZero: true } } }
        });
    }
});