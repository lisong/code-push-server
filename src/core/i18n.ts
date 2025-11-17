import path from 'path';
import { I18n } from 'i18n';

import en from '../locales/en.json';
import ko from '../locales/ko.json';
import zh from '../locales/zh.json';
import type { LocaleI18n } from './middleware';

const resources: Record<LocaleI18n, Record<string, string>> = {
    en,
    ko,
    zh,
};

export const i18n = new I18n();

i18n.configure({
    directory: path.join(__dirname, '../locales'),
    defaultLocale: 'en',
});

/**
 * @description 모던 i18next 라이브러리의 t함수와 유사함 동작 보장
 * - 이 프로젝트의 i18n 의 번역함수 사용법 예시
 * - e.g., req.__("greeting") OR res.__("greeting")
 * - 위 방법 외에 t("greeting") 호출로도 동일한 스트링 리턴 보장
 */
export function t(locale: LocaleI18n, key: string, vars?: Record<string, any>): string {
    const dict = resources[locale] ?? resources.zh;

    // 키에 해당하는 문장 (없으면 key 자체 반환)
    const template = dict[key] ?? key;

    // 변수 치환 없음 -> 그대로 반환
    if (!vars) {
        return template;
    }

    // Interpolation 처리: {{varName}} -> vars[varName]
    return template.replace(/\{\{\s*(\w+)\s*\}\}/g, (_, varName) => {
        if (vars[varName] == null) {
            return ''; // 없는 변수를 치환하면 빈문자열 (i18next 기본 동작과 유사)
        }
        return String(vars[varName]);
    });
}
