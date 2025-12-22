import { DataTypes, Model } from 'sequelize';
import { sequelize } from '../core/utils/connections';

interface UserTokensInterface extends Model {
    id: number;
    uid: number;
    name: string;
    tokens: string;
    description: string;
    is_session: number;
    created_by: string;
    created_at: Date;
    expires_at: Date;
}

export const UserTokens = sequelize.define<UserTokensInterface>(
    'UserTokens',
    {
        id: {
            type: DataTypes.BIGINT({ length: 20 }),
            allowNull: false,
            autoIncrement: true,
            primaryKey: true,
        },
        uid: DataTypes.BIGINT({ length: 20 }),
        name: DataTypes.STRING,
        tokens: DataTypes.STRING,
        description: DataTypes.STRING,
        is_session: DataTypes.INTEGER({ length: 3 }),
        created_by: DataTypes.STRING,
        created_at: DataTypes.DATE,
        expires_at: DataTypes.DATE,
    },
    {
        updatedAt: false,
        tableName: 'user_tokens',
        underscored: true,
        paranoid: true,
    },
);
