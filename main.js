const FormData = require('form-data');
const fs = require('fs');
const axios = require('axios');
async function uploadFile(bucketName, filePath) {
  const form = new FormData();
  form.append('file', fs.createReadStream(filePath));
  
  try {
    const response = await axios.post(`https://dashing-gopher-upright.ngrok-free.app/buckets/${bucketName}/files`, form, {
      headers: form.getHeaders(),
    });
    console.log(response.data);
  } catch (error) {
    console.error(error.response ? error.response.data : error.message);
  }
}

uploadFile(process.argv[2], process.argv[3]);