async function fetchData() {
    try {
      const response = await fetch("http://127.0.0.1:8000/data"); // backend endpoint
      const data = await response.json();
      make_table(data.merged);
      document.getElementById("output").textContent = data.result || "No result";
      
    } catch (err) {
      
      console.error(err);
      document.getElementById("output").textContent = "Error fetching data";
    }
  }

  function make_table(js_Table){
    const table = document.createElement("table");
    table.style.borderCollapse = "collapse";
    table.style.margin = "20px auto";
    const thread = document.createElement("thead");
    const hr = document.createElement("tr");
    Object.keys(js_Table[0]).forEach(key => {
        const th = document.createElement("th");
        th.textContent = key;
        th.style.padding = "8px";
        th.style.background = "#1e90ff";
        th.style.color = "white";
        hr.appendChild(th);
    });
    thread.appendChild(hr);
    table.appendChild(thread);
    const tbody = document.createElement("tbody");
    js_Table.forEach(row=> {
        const tr = document.createElement("tr");
        Object.values(row).forEach(value=> {
            const td = document.createElement("td")
            td.textContent = value;
            td.style.padding = "8px";
            td.style.border = "1px solid #ddd";
            tr.appendChild(td);
        });
        tbody.appendChild(tr);
    });
    table.appendChild(tbody);
    dmerged = document.getElementById("merged");
    dmerged.innerHTML = "";
    dmerged.appendChild(table);
  }
  
  

  document.getElementById("fetchBtn").addEventListener("click", fetchData);
  
  setInterval(fetchData, 60000*60);