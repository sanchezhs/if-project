const express = require("express");
const cors = require("cors");

const app = express();
app.use(cors({ origin: "http://localhost:3000" })); 
app.use(express.json());

app.use("/api/airbnb", require("./routes/airbnb"));

console.log(process.env.DB_PASSWORD)
const PORT = process.env.PORT || 5000;
app.listen(PORT, () => console.log(`Server running on port ${PORT}`));
