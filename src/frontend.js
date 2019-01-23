(function() {
  const serviceEndpoint = window._serviceEndpoint;
  const $distributionId = document.querySelector("#distributionId");
  const $bearerToken = document.querySelector("#bearerToken");
  const $file = document.querySelector("#file");
  const $upload = document.querySelector("#upload");
  const $error = document.querySelector(".error");

  function createDistribution(bearerToken, distributionId) {
    return fetch(serviceEndpoint, {
      method: "POST",
      headers: new Headers({
        Authorization: `Bearer ${bearerToken}`
      }),
      body: JSON.stringify({ distributionId })
    });
  }

  function postToS3(url, fields, file) {
    const formData = new FormData();
    for (const field in fields) {
      if (fields.hasOwnProperty(field)) {
        formData.append(field, fields[field]);
      }
    }
    formData.append("file", file);
    console.log("POST TO S3", url, formData);
    return fetch(url, {
      method: "POST",
      body: formData
    });
  }

  function error(msg) {
    console.error(msg);
    $error.textContent = msg;
  }

  function upload() {
    const distributionId = $distributionId.value;
    const bearerToken = $bearerToken.value;
    const files = $file.files;
    if (files.length !== 1) {
      error("Select 1 file");
      return;
    }
    const file = files[0];
    createDistribution(bearerToken, distributionId)
      .then(res => res.json())
      .then(data => postToS3(data.url, data.fields, file))
      .then(() => error("Uploaded?"))
      .catch(e => error(e));
  }

  $upload.addEventListener("click", upload);
})();
