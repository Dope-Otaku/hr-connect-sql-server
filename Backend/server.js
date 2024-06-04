const express = require("express");
const sql = require("msnodesqlv8");

const app = express();
app.use(express.json());

// SQL Server connection string
const connectionString =
  "server=RAJ\\SQLEXPRESS;Database=ITP_1482;Trusted_Connection=Yes;Driver={SQL Server Native Client 11.0}";

// Function to execute a query
const executeQuery = (query, callback) => {
  sql.query(connectionString, query, (err, rows) => {
    if (err) {
      console.error("SQL error", err);
      callback(err, null);
    } else {
      callback(null, rows);
    }
  });
};

// GET all data
app.get("/api/data", (req, res) => {
  const query = "SELECT * FROM dbo.ConnectedPLCs";
  executeQuery(query, (err, rows) => {
    if (err) {
      res.status(500).json({ error: "Failed to fetch data" });
    } else {
      res.json(rows);
    }
  });
});

// POST new data
app.post("/api/data", (req, res) => {
  const { column1, column2 /* ... */ } = req.body;
  const query = `
    INSERT INTO your_table_name (column1, column2, /* ... */)
    VALUES ('${column1}', '${column2}', /* ... */)
  `;
  executeQuery(query, (err, result) => {
    if (err) {
      res.status(500).json({ error: "Failed to insert data" });
    } else {
      res.json({ message: "Data inserted successfully" });
    }
  });
});

// Start the server
const port = process.env.PORT || 8002;
app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});
