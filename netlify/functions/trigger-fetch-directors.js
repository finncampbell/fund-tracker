// netlify/functions/trigger-fetch-directors.js

exports.handler = async function(event, context) {
  // CORS headers
  const CORS = {
    'Access-Control-Allow-Origin':  '*',
    'Access-Control-Allow-Methods': 'OPTIONS,POST',
    'Access-Control-Allow-Headers': 'Content-Type,Authorization'
  };

  // Preflight
  if (event.httpMethod === 'OPTIONS') {
    return { statusCode: 204, headers: CORS };
  }

  // Only POST
  if (event.httpMethod !== 'POST') {
    return {
      statusCode: 405,
      headers: CORS,
      body: 'Method Not Allowed'
    };
  }

  // Workflow dispatch details
  const owner       = 'finncampbell';
  const repo        = 'fund-tracker';
  const workflow_id = 'fetch-directors.yml';
  const token       = process.env.GITHUB_TOKEN;

  if (!token) {
    return {
      statusCode: 500,
      headers: CORS,
      body: 'Missing GITHUB_TOKEN in Netlify environment'
    };
  }

  const url = `https://api.github.com/repos/${owner}/${repo}/actions/workflows/${workflow_id}/dispatches`;
  const payload = { ref: 'main', inputs: {} };

  try {
    // Use the global fetch — no extra import needed
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
