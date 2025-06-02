// netlify/functions/trigger-fetch-directors.js

const fetch = require('node-fetch'); // Netlify includes this by default
exports.handler = async (event, context) => {
  // GitHub repo info – replace with your own
  const owner = 'YOUR_GITHUB_USERNAME_OR_ORG';
  const repo  = 'YOUR_REPO_NAME';

  // The workflow file we want to dispatch:
  const workflow_id = 'fetch-directors.yml';

  // Note: You must set GITHUB_TOKEN (with workflow dispatch permission) in Netlify's env
  const token = process.env.GITHUB_TOKEN;

  if (!token) {
    return {
      statusCode: 500,
      body: 'Missing GITHUB_TOKEN in Netlify environment',
    };
  }

  const url = `https://api.github.com/repos/${owner}/${repo}/actions/workflows/${workflow_id}/dispatches`;

  // For workflow_dispatch with no inputs, we send an object with "ref" only:
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
