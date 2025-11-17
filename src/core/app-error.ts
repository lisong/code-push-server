/* eslint-disable max-classes-per-file */
export class AppError extends Error {
    constructor(message: string | Error) {
        super(message instanceof Error ? message.message : message);
        this.name = 'AppError';
    }

    public status = 200;
}

/** i18n 에러 메시지 번역 포함 */
export class AppErrorI18n extends Error {
    public readonly messageKey: string;
    public readonly messageVars?: Record<string, any>;

    constructor(messageKey: string, messageVars?: Record<string, any>) {
        // Error.message에는 일단 key만 넣어둠 (실제 표현은 나중에 req.t로 처리)
        super(messageKey);

        this.messageKey = messageKey;
        this.messageVars = messageVars;

        Object.setPrototypeOf(this, new.target.prototype); // Error 상속시 필수 처리
        this.name = 'AppErrorI18n';
    }
}

export class NotFound extends AppError {
    constructor(message?: string | Error) {
        super(message || 'Not Found');
        this.name = 'NotFoundError';
    }

    public status = 404;
}

export class Unauthorized extends AppError {
    constructor(message?: string | Error) {
        super(message || 'Unauthorized');
        this.name = 'UnauthorizedError';
    }

    public status = 401;
}
