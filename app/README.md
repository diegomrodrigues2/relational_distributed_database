# Run and deploy your AI Studio app

This contains everything you need to run your app locally.

## Run Locally

**Prerequisites:**  Node.js


1. Install dependencies:
   `npm install`
2. Set the `GEMINI_API_KEY` in [.env.local](.env.local) to your Gemini API key
3. (Optional) Set `VITE_API_BASE` in [.env.local](.env.local) to the backend API base URL (defaults to `http://localhost:8000`)
4. Run the app:
   `npm run dev`
