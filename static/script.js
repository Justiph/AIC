document.addEventListener("DOMContentLoaded", () => {
    const imageInput = document.getElementById("imageInput");
    const imageContainer = document.getElementById("imageContainer");
    const imgBox1 = document.getElementById("imgbox1");
    const imgBox2 = document.getElementById("imgbox2");
    const resultRow = document.getElementById("resultrow");

    // Upload images
    imageInput.addEventListener("change", async () => {
        const files = imageInput.files;
        const formData = new FormData();
        for (let file of files) {
            formData.append("images", file);
        }
        await fetch("/upload", { method: "POST", body: formData });
        loadDB();
    });

    // Load DB images
    async function loadDB() {
        const res = await fetch("/db");
        const data = await res.json();
        imageContainer.innerHTML = "";
        data.images.forEach(filename => {
            const div = document.createElement("div");
            div.className = "col p-1";
            div.innerHTML = `<img src="/image/${filename}" class="img-fluid" />`;
            imageContainer.appendChild(div);
        });
    }

    loadDB();
});
