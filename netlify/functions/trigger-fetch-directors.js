// netlify/functions/trigger-fetch-directors.js
const fetch = require('node-fetch');

exports.handler = async (event, context) => {
  // Always allow CORS
  const CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization'
  };

  // 1) Handle CORS preflight
  if (event.httpMethod === 'OPTIONS') {
    return {
      statusCode: 204,
      headers: CORS_HEADERS
    };
  }

  // 2) Only allow POST
  if (event.httpMethod !== 'POST') {
    return {
      statusCode: 405,
      headers: CORS_HEADERS,
      body: 'Method Not Allowed'
    };
  }

  // 3) GitHub workflow dispatch
  const owner       = 'finncampbell';
  const repo        = 'fund-tracker';
  const workflow_id = 'fetch-directors.yml';
  const token       = process.env.GITHUB_TOKEN;

  if (!token) {
    return {
      statusCode: 500,
      headers: CORS_HEADERS,
      body: 'Missing GITHUB_TOKEN in Netlify environment'
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
        headers: CORS_HEADERS,
        body: 'Workflow dispatch triggered'
      };
    } else {
      const text = await resp.text();
      return {
        statusCode: resp.status,
        headers: CORS_HEADERS,
        body: `GitHub API responded with ${resp.status}: ${text}`
      };
    }
  } catch (err) {
    return {
      statusCode: 500,
      headers: CORS_HEADERS,
      body: `Error: ${err.message}`
    };
  }
};
