document.addEventListener("DOMContentLoaded", function() {
    // Get all image elements in the document
    var images = document.querySelectorAll('.foto-preview');

    // Attach click event listener to each image
    images.forEach(function(image) {
        image.addEventListener('click', function() {
            // Create a modal or open a new window with larger image preview
            window.open(image.getAttribute('href'), '_blank');
        });
    });
});
