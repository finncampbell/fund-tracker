// netlify/functions/trigger-fetch-directors.js
const fetch = require('node-fetch');

exports.handler = async (event, context) => {
  // GitHub repo info – replace with your own
  const owner = 'YOUR_GITHUB_USERNAME_OR_ORG';
  const repo  = 'YOUR_REPO_NAME';
  const workflow_id = 'fetch-directors.yml';

  // Ensure GITHUB_TOKEN is set in Netlify environment
  const token = process.env.GITHUB_TOKEN;
  if (!token) {
    return {
      statusCode: 500,
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
        body: 'Workflow dispatch triggered',
      };
    } else {
      const text = await resp.text();
      return {
        statusCode: resp.status,
        body: `GitHub API responded with ${resp.status}: ${text}`,
      };
    }
  } catch (err) {
    return {
      statusCode: 500,
      body: `Error: ${err.message}`,
    };
  }
};
