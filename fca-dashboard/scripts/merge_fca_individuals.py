name: Fetch FCA Firm Individuals (Ad-hoc)

permissions:
  contents: write

on:
  workflow_dispatch:
    inputs:
      chunks:
        description: 'Number of parallel chunks to split into'
        required: false
        default: '4'
      limit:
        description: 'How many FRNs per chunk (blank = full list / auto-calculated)'
        required: false
        default: ''

jobs:
  split:
    runs-on: ubuntu-latest
    outputs:
      chunk-list: ${{ steps.split.outputs.chunk-list }}
    steps:
      - uses: actions/checkout@v3

      - name: Read total FRNs and build chunk list
        id: split
        run: |
          total=$(jq '. | length' fca-dashboard/data/all_frns_with_names.json)
          chunks=${{ github.event.inputs.chunks }}
          # if no limit provided, auto-calc limit per chunk
          if [ -z "${{ github.event.inputs.limit }}" ]; then
            limit=$(( (total + chunks - 1) / chunks ))
          else
            limit=${{ github.event.inputs.limit }}
          fi
          # build JSON array of chunk offsets
          offsets=()
          for ((i=0; i<total; i+=limit)); do
            offsets+=($i)
          done
          echo "Built offsets: ${offsets[*]}"
          # emit as JSON array for matrix
          echo "::set-output name=chunk-list::[$(printf '%s\n' "${offsets[@]}" | paste -sd, -)]"

  fetch:
    needs: split
    runs-on: ubuntu-latest
    strategy:
      matrix:
        offset: ${{ fromJson(needs.split.outputs.chunk-list) }}
    steps:
      - uses: actions/checkout@v3

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: '3.x'

      - name: Install dependencies
        run: pip install requests

      - name: Fetch individuals chunk ${{ matrix.offset }}
        env:
          FCA_API_EMAIL: ${{ secrets.FCA_API_EMAIL }}
          FCA_API_KEY:   ${{ secrets.FCA_API_KEY }}
        run: |
          mkdir -p fca-dashboard/data/chunks
          python fca-dashboard/scripts/fetch_firm_individuals.py \
            --offset ${{ matrix.offset }} \
            --limit ${limit:-} \
          > fca-dashboard/data/chunks/part_${{ matrix.offset }}.json

  merge:
    needs: fetch
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Merge chunked results
        run: |
          python fca-dashboard/scripts/merge_fca_individuals.py \
            fca-dashboard/data/chunks \
            fca-dashboard/data/fca_individuals_by_firm.json

      - name: Commit & push merged file
        run: |
          git config user.name "github-actions[bot]"
          git config user.email "actions@github.com"
          git add fca-dashboard/data/fca_individuals_by_firm.json
          git diff --cached --quiet || git commit -m "chore(fca): merge firm individuals"
          git push origin HEAD:main
