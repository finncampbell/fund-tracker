# ============================================================================= 
# Workflow: Fetch FCA Firm Individuals (Ad-hoc, chunked + throttled)
# =============================================================================
name: Fetch FCA Firm Individuals (Ad-hoc)

# Grant write so we can commit back to main
permissions:
  contents: write

# Manual trigger only, with default of 5 parallel chunks
on:
  workflow_dispatch:
    inputs:
      chunks:
        description: 'Number of parallel chunks to split the FRN list into'
        required: true
        default: '5'
      limit:
        description: 'Max FRNs per chunk (blank = full slice)'
        required: false
        default: ''

jobs:
  # -----------------------------------------------------------------------------
  # JOB 1: split
  #   - Partition the master FRN list into start offsets
  #   - Emit both `chunk_list` and `chunk_count` for downstream jobs
  # -----------------------------------------------------------------------------
  split:
    runs-on: ubuntu-latest
    outputs:
      chunk_list: ${{ steps.set-chunks.outputs.chunk_list }}
      chunk_count: ${{ steps.set-chunks.outputs.chunk_count }}

    steps:
      - name: Check out repository    # bring in all_frns_with_names.json
        uses: actions/checkout@v3

      - id: set-chunks
        name: Compute chunk start indices
        run: |
          # 1. Count total FRNs
          TOTAL=$(jq 'length' fca-dashboard/data/all_frns_with_names.json)

          # 2. Desired number of chunks
          CHUNKS=${{ github.event.inputs.chunks }}

          # 3. Compute spaced offsets via Python
          python3 -c "import math,json; total=int('$TOTAL'); n=int('$CHUNKS'); size=math.ceil(total/n); print(json.dumps(list(range(0,total,size))))" \
            > indices.json

          # 4. Expose offsets for the fetch matrix
          echo "chunk_list=$(cat indices.json)" >> $GITHUB_OUTPUT

          # 5. Expose actual chunk count for rate-limit calc
          echo "chunk_count=$(jq 'length' indices.json)" >> $GITHUB_OUTPUT

  # -----------------------------------------------------------------------------
  # JOB 2: fetch
  #   - Parallel matrix: each worker fetches one slice of FRNs
  #   - Throttles API calls to floor(50/COUNT) per 10s per runner
  # -----------------------------------------------------------------------------
  fetch:
    needs: split
    runs-on: ubuntu-latest
    strategy:
      matrix:
        start: ${{ fromJson(needs.split.outputs.chunk_list) }}

    steps:
      - name: Check out repository          # get code & data
        uses: actions/checkout@v3

      - name: Set up Python                # ensure python3 + pip
        uses: actions/setup-python@v4
        with:
          python-version: '3.x'

      - name: Install dependencies         # install requests for API
        run: pip install requests

      - name: Fetch individuals chunk (throttled)
        env:
          FCA_API_EMAIL: ${{ secrets.FCA_API_EMAIL }}
          FCA_API_KEY:   ${{ secrets.FCA_API_KEY }}
        run: |
          # 1. Compute this worker’s offset & total chunks
          OFFSET=${{ matrix.start }}
          COUNT=${{ needs.split.outputs.chunk_count }}

          # 2. Compute per-worker rate limits
          RL_MAX_CALLS=$((50 / COUNT))
          RL_WINDOW_S=10
          echo "🔢 Rate limit per worker: $RL_MAX_CALLS calls per $RL_WINDOW_S seconds"
          export RL_MAX_CALLS RL_WINDOW_S

          # 3. Pass test limit if provided
          if [ -n "${{ github.event.inputs.limit }}" ]; then
            LIMIT="--limit ${{ github.event.inputs.limit }}"
          else
            LIMIT=""
          fi

          # 4. Prepare output directory
          OUT_DIR="chunks/individuals-chunk-${OFFSET}"
          mkdir -p "$OUT_DIR"

          # 5. Run the chunked fetch script
          python3 fca-dashboard/scripts/fetch_firm_individuals.py \
            --offset "$OFFSET" $LIMIT \
            --output "$OUT_DIR/fca_individuals_by_firm.json"

      - name: Upload chunk artifact         # save partial JSON
        uses: actions/upload-artifact@v4
        with:
          name: individuals-chunk-${{ matrix.start }}
          path: chunks/individuals-chunk-${{ matrix.start }}/fca_individuals_by_firm.json

  # -----------------------------------------------------------------------------
  # JOB 3: merge
  #   - Download all chunk artifacts
  #   - Merge into one UI JSON under docs/, then commit & rebase before pushing
  # -----------------------------------------------------------------------------
  merge:
    needs: fetch
    runs-on: ubuntu-latest

    steps:
      - name: Check out repository (full history)
        uses: actions/checkout@v3
        with:
          fetch-depth: 0
          persist-credentials: true

      - name: Download chunk artifacts       # pull all chunk outputs
        uses: actions/download-artifact@v4
        with:
          path: chunks

      - name: Merge into UI data folder      # combine into docs JSON
        run: |
          mkdir -p docs/fca-dashboard/data
          python3 fca-dashboard/scripts/merge_fca_individuals.py \
            chunks docs/fca-dashboard/data/fca_individuals_by_firm.json

      - name: Commit & push merged result    # commit and rebase to main
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "actions@github.com"

          # Stage the updated JSON
          git add docs/fca-dashboard/data/fca_individuals_by_firm.json

          # Commit if there are changes
          git diff --cached --quiet || git commit -m "chore(fca): merge firm individuals into docs"

          # Rebase onto latest remote main to allow fast-forward push
          git pull --rebase origin main

          # Push commits (merge commit + any rebased)
          git push origin HEAD:main
