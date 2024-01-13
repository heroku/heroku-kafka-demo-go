let resolve = (path, obj) => {
  return path.split(".").reduce((prev, curr) => {
    return prev ? prev[curr] : null;
  }, obj || self);
};

let fetchMessages = () => {
  let headers = new Headers();
  headers.append("Content-Type", "application/json");
  headers.append("Accept", "application/json");

  let init = {
    method: "GET",
    headers: headers,
    mode: "cors",
    cache: "default",
  };

  let request = new Request("/messages", init);
  fetch(request)
    .then((response) => response.json())
    .then((data) => {
      let table = document.querySelector(".messages");
      table.replaceChildren();

      let thead = document.createElement("thead");
      let tbody = document.createElement("tbody");

      table.appendChild(thead);
      table.appendChild(tbody);

      let row = document.createElement("tr");
      ["Offset", "Partition", "Message", "Recieved At"].forEach((header) => {
        let headerElement = document.createElement("th");
        headerElement.textContent = header;
        row.appendChild(headerElement);
      });

      thead.appendChild(row);

      if (data === null || data.length === 0) {
        return;
      }

      for (let i = 0; i < data.length; i++) {
        let row = document.createElement("tr");

        ["offset", "partition", "value", "metadata.receivedAt"].forEach(
          (path) => {
            let cell = document.createElement("td");
            cell.textContent = resolve(path, data[i]);
            row.appendChild(cell);
          }
        );

        tbody.appendChild(row);
      }
    })
    .then(() => {
      setTimeout(fetchMessages, 5000);
    })
    .catch((error) => {
      console.error(error);
    });
};

fetchMessages();
