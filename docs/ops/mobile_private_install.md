# Mobile Private Install

## Goal

- Public URL を使わずに iPhone へ mobile learning shell を入れること
- 初回インストール後は自宅 PC を起動していなくても使えること

## Stop Public Pages

1. GitHub repository の `Settings`
2. `Pages`
3. `Stop publishing`

必要なら `.github/workflows/mobile_pwa_pages.yml` は手動実行のみ。

## Prepare iOS Shell

```bash
scripts/ops/prepare_mobile_ios_shell.sh
```

これで `frontend/dist` が `ios/MobileLearningShell/WebApp` に同期される。
`WebApp` 配下は generated resource で、Git 追跡対象ではない。

## Open In Xcode

1. `ios/MobileLearningShell.xcodeproj` を Xcode で開く
2. Target `MobileLearningShell` を選ぶ
3. `Signing & Capabilities` で自分の Team を選ぶ
4. `Bundle Identifier` を必要なら一意な値に変える

## Install To iPhone

1. iPhone を Mac に接続
2. Xcode 上部の run destination で iPhone を選ぶ
3. `Run`
4. iPhone 側で developer trust が必要なら許可

## Verification

- ホーム画面に `Switchyard Mobile` が出ること
- 機内モードでも起動できること
- `Home` `Cards` `Drill` `Risk` が表示されること

## Notes

- この shell は `frontend/dist` を app bundle に同梱して起動する
- network が無くても mobile learning の on-device pack で動く
- live backend 参照は含まない
