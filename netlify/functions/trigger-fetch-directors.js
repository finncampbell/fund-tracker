// netlify/functions/trigger-fetch-directors.js
const fetch = require('node-fetch');

exports.handler = async (event, context) => {
  // Your GitHub repo info
  const owner       = 'finncampbell';
  const repo        = 'fund-tracker';
  const workflow_id = 'fetch-directors.yml';

  // Ensure GITHUB_TOKEN is set in Netlify
  const token = process.env.GITHUB_TOKEN;
  if (!token) {
    return {
      statusCode: 500,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: 'Missing GITHUB_TOKEN in Netlify environment',
    };
  }

  const url = `https://api.github.com/repos/${owner}/${repo}/actions/workflows/${workflow_id}/dispatches`;
  const payload = {
    ref: 'main',
    inputs: {}                     // include empty inputs object
  };

  try {
    const resp = await fetch(url, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${token}`,
        'Accept':        'application/vnd.github.v3+json',
        'Content-Type':  'application/json'      // <— added
      },
      body: JSON.stringify(payload)
    });

    if (resp.status === 204) {
      return {
        statusCode: 200,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: 'Workflow dispatch triggered'
      };
    } else {
      const text = await resp.text();
      return {
        statusCode: resp.status,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: `GitHub API responded with ${resp.status}: ${text}`
      };
    }
  } catch (err) {
    return {
      statusCode: 500,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: `Error: ${err.message}`
    };
  }
};
