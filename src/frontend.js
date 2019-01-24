(function() {
  "use strict";

  const generateS3UrlEndpoint = window._generateS3UrlEndpoint;
  const metadataApiEndpoint = window._metadataApiEndpoint;

  // Glorious norwenglish mix
  const $bearerToken = document.querySelector("#bearerToken");
  const $datasettId = document.querySelector("#datasettId");
  const $skjemaId = document.querySelector("#skjemaId");
  const $startTime = document.querySelector("#startTime");
  const $endTime = document.querySelector("#endTime");
  const $description = document.querySelector("#description");
  const $file = document.querySelector("#file");
  const $upload = document.querySelector("#upload");
  const $message = document.querySelector(".message");

  function doesDatasettExist(datasettId) {
    return fetch(`${metadataApiEndpoint}/datasets/${datasettId}`)
      .then(res => res.json())
      .then(data => {
        if (!Array.isArray(data) || data.length === 0) {
          throw new Error("Datasett does not exist");
        }
        console.log("Found datasett", data);
      });
  }

  function doesVersionExist(datasettId, versionId) {
    return fetch(
      `${metadataApiEndpoint}/datasets/${datasettId}/versions/${versionId}`
    )
      .then(res => res.json())
      .then(data => {
        if (!Array.isArray(data) || data.length === 0) {
          throw new Error("Datasett does not exist");
        }
        console.log("Found version", data);
      });
  }

  function createEdition(datasettId, versionId, edition) {
    return fetch(
      `${metadataApiEndpoint}/datasets/${datasettId}/versions/${versionId}/editions`,
      {
        method: "POST",
        body: JSON.stringify(edition)
      }
    )
      .then(res => res.json())
      .then(editionId => {
        console.log("Created edition", editionId);
        return editionId;
      });
  }

  function generateS3URL(
    bearerToken,
    datasettId,
    versionId,
    editionId,
    filename
  ) {
    return fetch(generateS3UrlEndpoint, {
      method: "POST",
      headers: new Headers({
        Authorization: `Bearer ${bearerToken}`
      }),
      body: JSON.stringify({
        datasettId,
        versionId,
        editionId,
        filename
      })
    })
      .then(res => {
        if (res.ok) {
          return res.json();
        }
        throw new Error("Failed to generate S3 URL");
      })
      .then(data => {
        console.log("Generated S3 URL", data);
        return data;
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
    })
      .then(res => {
        if (res.ok) {
          return res.json();
        }
        throw new Error("Failed to POST to S3");
      })
      .then(data => {
        console.log("Posted to S3", data);
        return data;
      });
  }

  function message(msg) {
    console.log(msg);
    $message.textContent = msg;
  }

  function error(msg) {
    message(msg);
    if (!$message.classList.contains("error")) {
      $message.classList.add("error");
    }
  }

  function info(msg) {
    message(msg);
    if ($message.classList.contains("error")) {
      $message.classList.remove("error");
    }
  }

  function upload() {
    const files = $file.files;
    if (files.length !== 1) {
      error("Select 1 file");
      return;
    }
    const file = files[0];
    console.log("Try upload file", file);

    const bearerToken = $bearerToken.value;
    const datasettId = $datasettId.value;
    const skjemaId = $skjemaId.value;
    const startTime = $startTime.value;
    const endTime = $endTime.value;
    const description = $description.value;

    Promise.all([
      doesDatasettExist(datasettId),
      doesVersionExist(datasettId, skjemaId)
    ])
      .then(() =>
        createEdition(datasettId, skjemaId, {
          startTime,
          endTime,
          description
        })
      )
      .then(editionId =>
        generateS3URL(bearerToken, datasettId, skjemaId, editionId, file.name)
      )
      .then(s3data => postToS3(s3data.url, s3data.fields, file))
      .then(() => info("Uploaded?"))
      .catch(error);
  }

  $upload.addEventListener("click", upload);
})();
