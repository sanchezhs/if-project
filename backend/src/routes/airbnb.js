const express = require("express");
const router = express.Router();
const pool = require("../db");

router.get("/", async (req, res) => {
  try {
    const result = await pool.query(`SELECT 
    *
  FROM airbnb a 
  JOIN provincias p on a.provincia_id = p.id
  LIMIT 10000;`
);
    res.json(result.rows);
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

module.exports = router;
