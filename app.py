from flask import Flask, request, jsonify, send_from_directory, render_template
import os
import random

app = Flask(__name__)

# Folder for demo images
IMAGE_FOLDER = os.path.join("static", "images")
os.makedirs(IMAGE_FOLDER, exist_ok=True)

@app.route("/")
def home():
    return render_template("index.html")

@app.route("/db")
def list_db():
    images = os.listdir(IMAGE_FOLDER)
    return jsonify({"images": images})

@app.route("/image/<filename>")
def get_image(filename):
    return send_from_directory(IMAGE_FOLDER, filename)

@app.route("/upload", methods=["POST"])
def upload_images():
    files = request.files.getlist("images")
    for file in files:
        file.save(os.path.join(IMAGE_FOLDER, file.filename))
    return jsonify({"status": "ok"})

@app.route("/query", methods=["POST"])
def query_image():
    images = os.listdir(IMAGE_FOLDER)
    if not images:
        return jsonify({"error": "No images in DB"}), 404
    best_match = random.choice(images)
    return jsonify({"filename": best_match})

if __name__ == "__main__":
    app.run(debug=True)
