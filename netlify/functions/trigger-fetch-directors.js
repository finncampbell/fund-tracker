// netlify/functions/trigger-fetch-directors.js
const fetch = require('node-fetch');

exports.handler = async (event, context) => {
  // Update these to match your GitHub repo
  const owner       = 'finncampbell';       // your GitHub username or org
  const repo        = 'fund-tracker';       // your repository name
  const workflow_id = 'fetch-directors.yml'; // the workflow file name exactly

  // Ensure you have set GITHUB_TOKEN in Netlify environment (see steps below)
  const token = process.env.GITHUB_TOKEN;
  if (!token) {
    return {
      statusCode: 500,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: 'Missing GITHUB_TOKEN in Netlify environment',
    };
  }

  const url = `https://api.github.com/repos/${owner}/${repo}/actions/workflows/${workflow_id}/dispatches`;
  const body = JSON.stringify({ ref: 'main' });

  try {
    const resp = await fetch(url, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${token}`,
        Accept: 'application/vnd.github.v3+json',
      },
      body,
    });

    if (resp.status === 204) {
      return {
        statusCode: 200,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: 'Workflow dispatch triggered',
      };
    } else {
      const text = await resp.text();
      return {
        statusCode: resp.status,
        headers: { 'Access-Control-Allow-Origin': '*' },
        body: `GitHub API responded with ${resp.status}: ${text}`,
      };
    }
  } catch (err) {
    return {
      statusCode: 500,
      headers: { 'Access-Control-Allow-Origin': '*' },
      body: `Error: ${err.message}`,
    };
  }
};
