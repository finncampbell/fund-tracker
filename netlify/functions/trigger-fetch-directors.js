// netlify/functions/trigger-fetch-directors.js
const fetch = require('node-fetch');

exports.handler = async (event, context) => {
  // 1) CORS headers to allow GitHub Pages origin to hit us
  const CORS = {
    'Access-Control-Allow-Origin':  '*',
    'Access-Control-Allow-Methods': 'OPTIONS,POST',
    'Access-Control-Allow-Headers': 'Content-Type,Authorization'
  };

  // 2) Handle preflight
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers: CORS };
  }

  // 3) Only allow POST
  if (event.httpMethod !== 'POST') {
    return { statusCode: 405, headers: CORS, body: 'Method Not Allowed' };
  }

  // 4) Dispatch GitHub Action
  const owner       = 'finncampbell';
  const repo        = 'fund-tracker';
  const workflow_id = 'fetch-directors.yml';
  const token       = process.env.GITHUB_TOKEN;

  if (!token) {
    return {
      statusCode: 500,
      headers: CORS,
      body: 'Missing GITHUB_TOKEN'
    };
  }

  const url = `https://api.github.com/repos/${owner}/${repo}/actions/workflows/${workflow_id}/dispatches`;
  const payload = { ref: 'main', inputs: {} };

  try {
    const resp = await fetch(url, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Accept':        'application/vnd.github.v3+json',
        'Content-Type':  'application/json'
      },
      body: JSON.stringify(payload)
    });

    if (resp.status === 204) {
      return {
        statusCode: 200,
        headers: CORS,
        body: 'Workflow dispatch triggered'
      };
    } else {
      const text = await resp.text();
      return {
        statusCode: resp.status,
        headers: CORS,
        body: `GitHub API error ${resp.status}: ${text}`
      };
    }
  } catch (err) {
    return {
      statusCode: 500,
      headers: CORS,
      body: `Exception: ${err.message}`
    };
  }
};
