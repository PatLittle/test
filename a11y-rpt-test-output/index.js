const fs = require('fs');
const path = require('path');

function getAllFiles(dirPath, arrayOfFiles) {
    files = fs.readdirSync(dirPath);

    arrayOfFiles = arrayOfFiles || [];

    files.forEach(function(file) {
        if (fs.statSync(dirPath + "/" + file).isDirectory()) {
            arrayOfFiles = getAllFiles(dirPath + "/" + file, arrayOfFiles);
        } else {
            arrayOfFiles.push(path.join(__dirname, dirPath, "/", file));
        }
    });

    return arrayOfFiles;
}

function generateLinks() {
    const files = getAllFiles(".");
    const fileList = document.getElementById("file-list");

    files.forEach(function(file) {
        const li = document.createElement("li");
        const a = document.createElement("a");
        a.href = file;
        a.textContent = file;
        li.appendChild(a);
        fileList.appendChild(li);
    });
}

window.onload = generateLinks;
