// App.js
import React, { useState, useEffect } from "react";
import axios from "axios";

function App() {
  const [data, setData] = useState([]);
  const [formData, setFormData] = useState({});

  useEffect(() => {
    fetchData();
  }, []);

  const fetchData = async () => {
    try {
      const response = await axios.get("/api/data");
      setData(response.data);
    } catch (error) {
      console.error("Error fetching data:", error);
    }
  };

  const handleSubmit = async (e) => {
    e.preventDefault();
    try {
      await axios.post("/api/data", formData);
      fetchData();
      setFormData({});
    } catch (error) {
      console.error("Error submitting data:", error);
    }
  };

  const handleChange = (e) => {
    setFormData({ ...formData, [e.target.name]: e.target.value });
  };

  return (
    <div>
      <form onSubmit={handleSubmit}>
        {/* Form inputs */}
        <button type="submit">Submit</button>
      </form>
      <table>
        <thead>
          <tr>{/* Table headers */}</tr>
        </thead>
        <tbody>
          {data.map((item) => (
            <tr key={item.id}>{/* Table row with data */}</tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default App;
